/*
 * openScale
 * Copyright (C) 2025 olie.xdev <olie.xdeveloper@googlemail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.health.openscale.core.bluetooth.scales

import com.health.openscale.core.bluetooth.data.ScaleMeasurement
import com.health.openscale.core.bluetooth.data.ScaleUser
import com.health.openscale.core.bluetooth.libs.YunmaiLib
import com.health.openscale.core.service.ScannedDeviceInfo
import com.health.openscale.core.utils.LogManager
import java.util.Date
import java.util.Locale
import java.util.UUID

/**
 * Handler for ES-CS20M scales (Yunmai / Lefu lineage).
 *
 * The scale exposes TWO vendor services:
 *
 *  1. 0x1A10 (Lefu/Renpho weight service)
 *       0x2A11 WRITE – "start measurement" and "delete history" commands
 *       0x2A10 NOTIFY – weight frames (0x11 start/stop, 0x14 weight)
 *     → Delivers weight only; impedance bytes are always 0x0000 in 0x14 frames.
 *
 *  2. 0xFFF0 (QN-style body-composition service)
 *       0xFFF1 NOTIFY – QN protocol frames (0x12 scale info, 0x10 weight + resistance)
 *       0xFFF2 WRITE  – QN protocol commands (0x13 unit config, 0x02 time sync)
 *     → Delivers weight + resistance (impedance) for body-composition calculation.
 *
 * Protocol flow:
 *   onConnected → setNotifyOn(0x2A10) + setNotifyOn(0xFFF1)
 *               → write 0x90 start to 0x2A11
 *               → write 0x95 delete-history to 0x2A11
 *   scale → 0x11 START (0x2A10)
 *   scale → 0x12 scale-info (0xFFF1) → app replies: 0x13 unit-cfg + 0x02 time-sync to 0xFFF2
 *   scale → 0x14 weight frames (0x2A10, many)
 *   scale → 0x10 stable weight+resistance (0xFFF1)  ← impedance arrives here
 *   scale → 0x11 STOP (0x2A10)  → parseAllFramesAndPublish()
 *
 * Body composition (fat%, water%, muscle%, bone, LBM, visceral fat) is computed
 * via YunmaiLib using the resistance value received on 0xFFF1.
 */
class ESCS20mHandler : ScaleDeviceHandler() {

    companion object {
        private const val TAG = "ESCS20mHandler"

        // Message IDs (byte[2] in 0x1A10 / Lefu frames)
        private const val MSG_START_STOP_RESP: Int = 0x11
        private const val MSG_WEIGHT_RESP:     Int = 0x14
        private const val MSG_EXTENDED_RESP:   Int = 0x15

        // START/STOP indicator position in MSG_START_STOP_RESP frames
        // byte[5] = 0x01 -> START, byte[5] = 0x00 -> STOP
        private const val START_STOP_FLAG_INDEX: Int = 5

        // QN epoch offset: seconds between Unix epoch (1970) and QN epoch (2000-01-01 UTC)
        private const val QN_EPOCH_OFFSET_SEC = 946_702_800L
    }

    // ── Lefu / 0x1A10 service ────────────────────────────────────────────────
    private val SVC_MAIN      = uuid16(0x1A10)
    private val CHR_CUR_TIME  = uuid16(0x2A11) // control / command mailbox (WRITE only)
    private val CHR_RESULTS   = uuid16(0x2A10) // notifications with results

    // ── QN-style / 0xFFF0 service ────────────────────────────────────────────
    private val SVC_FFF0 = uuid16(0xFFF0)
    private val CHR_FFF1 = uuid16(0xFFF1) // QN notify: 0x12 scale-info, 0x10 weight+resistance
    private val CHR_FFF2 = uuid16(0xFFF2) // QN write:  0x13 unit-cfg, 0x02 time-sync

    // "Magic" commands for the 0x1A10 service.
    // NOTE: the 0x90 payload MUST remain [01 00 00 00]; the scale rejects any other payload.
    // User profile (sex/height/age) is communicated via the QN 0x13 command on 0xFFF2 instead.
    private val MAGIC_START_MEAS = byteArrayOf(
        0x55, 0xAA.toByte(), 0x90.toByte(), 0x00, 0x04, 0x01, 0x00, 0x00, 0x00, 0x94.toByte()
    )
    private val MAGIC_DELETE_HISTORY = byteArrayOf(
        0x55, 0xAA.toByte(), 0x95.toByte(), 0x00, 0x01, 0x01, 0x96.toByte()
    )

    // ── Session state ────────────────────────────────────────────────────────
    private val rawFrames = mutableListOf<ByteArray>()  // 0x1A10 frame buffer
    private val acc = ScaleMeasurement()

    // QN-protocol state
    private var qnProtocolType: Byte = 0x00  // captured from 0x12 frame; echoed in 0x13 reply
    private var qnResistance: Int = 0        // resistance (Ω) received on 0xFFF1 0x10 frame

    // ── Device identification ────────────────────────────────────────────────

    override fun supportFor(device: ScannedDeviceInfo): DeviceSupport? {
        val name = device.name.lowercase(Locale.ROOT)
        val hasSvc = device.serviceUuids.any { it == SVC_MAIN }
        val looksEscs20m = hasSvc || name.contains("ES-CS20M".lowercase())

        if (!looksEscs20m) return null

        val caps = setOf(
            DeviceCapability.BODY_COMPOSITION,
            DeviceCapability.LIVE_WEIGHT_STREAM
        )
        return DeviceSupport(
            displayName = "ES-CS20M",
            capabilities = caps,
            implemented  = caps,
            linkMode     = LinkMode.CONNECT_GATT
        )
    }

    // ── Connection sequencing ────────────────────────────────────────────────

    override fun onConnected(user: ScaleUser) {
        rawFrames.clear()
        resetAccumulator()

        // Subscribe to the Lefu weight service
        setNotifyOn(SVC_MAIN, CHR_RESULTS)

        // Subscribe to the QN body-composition service (impedance arrives here)
        setNotifyOn(SVC_FFF0, CHR_FFF1)

        // Kick off a weight-measurement session on the Lefu service.
        // The 0x90 payload [01 00 00 00] must stay as-is; the scale rejects other payloads.
        writeTo(SVC_MAIN, CHR_CUR_TIME, MAGIC_START_MEAS)
        writeTo(SVC_MAIN, CHR_CUR_TIME, MAGIC_DELETE_HISTORY)

        LogManager.i(TAG, "Session started; waiting for frames…")
    }

    // ── Notification handling ────────────────────────────────────────────────

    override fun onNotification(characteristic: UUID, data: ByteArray, user: ScaleUser) {
        when (characteristic) {
            CHR_FFF1 -> handleQnFrame(data, user)
            CHR_RESULTS, CHR_CUR_TIME -> handleLefuFrame(data, user)
            else -> LogManager.d(TAG, "Notify from unrelated chr=$characteristic len=${data.size}")
        }
    }

    // ── 0xFFF1 / QN-protocol frame handler ───────────────────────────────────

    /**
     * Handle frames arriving on 0xFFF1 (QN-style body-composition service).
     *
     * 0x12 scale-info  → capture protocol type, send 0x13 unit-cfg + 0x02 time-sync to 0xFFF2.
     * 0x10 live frame  → when stable flag set, extract resistance for body-composition calc.
     */
    private fun handleQnFrame(data: ByteArray, user: ScaleUser) {
        if (data.isEmpty()) return

        val hex = data.joinToString(" ") { "%02X".format(it.toInt() and 0xFF) }
        LogManager.d(TAG, "QN 0xFFF1 len=${data.size}: [$hex]")

        when (data[0].toInt() and 0xFF) {
            0x12 -> {
                // Scale-info frame: capture protocol type, then send QN configuration
                qnProtocolType = if (data.size > 2) data[2] else 0x00
                LogManager.d(TAG, "QN 0x12 scale-info: protocol=0x%02X".format(qnProtocolType.toInt() and 0xFF))
                sendQnConfig(user)
            }
            0x10 -> {
                // Live weight + resistance frame.
                // Original QN format: [opcode][len?][proto][weight_hi][weight_lo][stable][r1_hi][r1_lo][r2_hi][r2_lo]
                if (data.size < 10) {
                    LogManager.d(TAG, "QN 0x10 frame too short (${data.size} bytes)")
                    return
                }
                val stable = data[5].toInt() == 1
                if (!stable) {
                    LogManager.d(TAG, "QN 0x10 unstable weight frame; skipping")
                    return
                }
                val resistance = ((data[6].toInt() and 0xFF) shl 8) or (data[7].toInt() and 0xFF)
                LogManager.d(TAG, "QN 0x10 stable: resistance=$resistance Ω (bytes[6:7])")
                if (resistance > 0) {
                    qnResistance = resistance
                }
            }
            else -> LogManager.d(TAG, "QN unhandled opcode=0x%02X".format(data[0].toInt() and 0xFF))
        }
    }

    /**
     * Send the QN unit-configuration and time-sync commands to 0xFFF2.
     * Must be called after receiving the 0x12 scale-info frame so [qnProtocolType] is known.
     */
    private fun sendQnConfig(user: ScaleUser) {
        val unitByte: Byte = 0x01 // kg (0x02 = lb)

        // 0x13 unit-configuration frame
        val cfg = byteArrayOf(0x13, 0x09, qnProtocolType, unitByte, 0x10, 0x00, 0x00, 0x00, 0x00)
        cfg[cfg.lastIndex] = qnChecksum(cfg)
        writeTo(SVC_FFF0, CHR_FFF2, cfg, withResponse = false)

        // 0x02 time-sync frame (seconds since QN epoch 2000-01-01, little-endian)
        val epochSecs = ((System.currentTimeMillis() / 1000L) - QN_EPOCH_OFFSET_SEC).toInt()
        val timeSync = byteArrayOf(
            0x02,
            (epochSecs and 0xFF).toByte(),
            ((epochSecs ushr 8)  and 0xFF).toByte(),
            ((epochSecs ushr 16) and 0xFF).toByte(),
            ((epochSecs ushr 24) and 0xFF).toByte()
        )
        writeTo(SVC_FFF0, CHR_FFF2, timeSync, withResponse = false)

        LogManager.d(TAG, "QN config sent: unit=kg, time-sync epochSecs=$epochSecs")
    }

    /** Sum-of-bytes checksum used by the QN protocol (excludes the last byte). */
    private fun qnChecksum(buf: ByteArray): Byte {
        var s = 0
        for (i in 0 until buf.size - 1) s = (s + (buf[i].toInt() and 0xFF)) and 0xFF
        return s.toByte()
    }

    // ── 0x2A10 / Lefu-protocol frame handler ─────────────────────────────────

    /**
     * Buffer all Lefu frames; take action only on 0x11 START/STOP frames.
     *
     * Protocol (from captured traffic):
     *   START frame: 55 AA 11 00 0A 01 01 01 00 00 39 00 00 00 00 56  (byte[5] = 0x01)
     *   STOP  frame: 55 AA 11 00 0A 00 01 01 00 00 39 00 00 00 00 55  (byte[5] = 0x00)
     */
    private fun handleLefuFrame(data: ByteArray, user: ScaleUser) {
        rawFrames += data.copyOf()

        if (data.size < 3) return
        val msgId = data[2].toInt() and 0xFF
        if (msgId != MSG_START_STOP_RESP) return

        if (data.size < 6) return
        val startStopFlag = data[START_STOP_FLAG_INDEX].toInt() and 0xFF

        when {
            startStopFlag != 0 -> LogManager.d(TAG, "Measurement started (flag=$startStopFlag)")
            else -> {
                LogManager.d(TAG, "Measurement stopped (flag=$startStopFlag) → parse & publish")
                parseAllFramesAndPublish(user)
            }
        }
    }

    override fun onDisconnected() {
        rawFrames.clear()
        resetAccumulator()
    }

    // ── Parsing and publishing ────────────────────────────────────────────────

    private fun parseAllFramesAndPublish(user: ScaleUser) {
        if (rawFrames.isEmpty()) {
            LogManager.w(TAG, "No frames buffered; nothing to publish.")
            return
        }

        val sex = if (user.gender.isMale()) 1 else 0
        val yunmai = YunmaiLib(sex, user.bodyHeight, user.activityLevel)

        // Sort frames by msgId (legacy behaviour)
        val frames = rawFrames.sortedBy { (it.getOrNull(2)?.toInt() ?: 0) and 0xFF }
        val weightFrameCount = frames.count { it.size >= 3 && (it[2].toInt() and 0xFF) == MSG_WEIGHT_RESP }
        LogManager.d(TAG, "Parsing ${frames.size} frames ($weightFrameCount weight frames)…")

        frames.forEach { parseFrame(it, yunmai, user) }

        // If 0x1A10 frames carried no resistance (common for this scale), try the QN resistance
        // received on 0xFFF1. This is the primary path for body-composition on the ES-CS20M.
        if (acc.fat == 0f && qnResistance > 0) {
            LogManager.d(TAG, "Using QN resistance: $qnResistance Ω for body-composition calculation")
            applyExtended(qnResistance, yunmai, user)
        }

        if (acc.weight > 0f) {
            acc.userId = user.id
            if (acc.dateTime == null) acc.dateTime = Date()
            LogManager.i(TAG, "Publishing measurement: weight=${acc.weight} kg, fat=${acc.fat}%")
            publish(snapshot(acc))
        } else {
            LogManager.w(TAG, "No valid weight decoded from $weightFrameCount frames; skip publishing.")
        }

        rawFrames.clear()
        resetAccumulator()
    }

    private fun snapshot(m: ScaleMeasurement) = ScaleMeasurement().apply {
        userId      = m.userId
        dateTime    = m.dateTime
        weight      = m.weight
        fat         = m.fat
        muscle      = m.muscle
        water       = m.water
        bone        = m.bone
        lbm         = m.lbm
        visceralFat = m.visceralFat
    }

    private fun parseFrame(frame: ByteArray, calc: YunmaiLib, user: ScaleUser) {
        if (frame.size < 3) return
        when ((frame[2].toInt() and 0xFF)) {
            MSG_WEIGHT_RESP   -> parseWeightFrame(frame, calc, user)
            MSG_EXTENDED_RESP -> parseExtendedFrame(frame, calc, user)
        }
    }

    /**
     * Weight frame (0x14).
     *
     * Protocol:
     *   55 AA 14 00 07 00 00 00 [w_hi] [w_lo] [r_hi] [r_lo] [checksum]
     *   bytes[8..9]  = weight, big-endian, units of 0.01 kg
     *   bytes[10..11] = optional embedded resistance (0x0000 if weight-only mode)
     */
    private fun parseWeightFrame(msg: ByteArray, calc: YunmaiLib, user: ScaleUser) {
        if (msg.size < 12) return

        val weightRaw = u16be(msg, 8)
        val weightKg = weightRaw / 100.0f

        if (weightKg < 0.5f || weightKg > 300f) {
            LogManager.d(TAG, "Ignoring unreasonable weight: $weightKg kg (raw=$weightRaw)")
            return
        }

        acc.weight = weightKg

        // Embedded resistance in this frame?
        val hasEmbedded = (msg[10].toInt() and 0xFF) != 0 || (msg[11].toInt() and 0xFF) != 0
        val hasSeparateExt = rawFrames.any { it.size >= 3 && ((it[2].toInt() and 0xFF) == MSG_EXTENDED_RESP) }

        if (hasEmbedded && !hasSeparateExt) {
            val resistance = u16be(msg, 10)
            LogManager.d(TAG, "Embedded resistance in 0x14 frame: $resistance Ω")
            applyExtended(resistance, calc, user)
        } else if (!hasEmbedded) {
            LogManager.d(TAG, "No resistance in 0x14 frame (bytes[10:11]=0x0000); weight-only frame")
        }
    }

    /**
     * Extended frame (0x15): resistance at bytes[9..10] (big-endian).
     */
    private fun parseExtendedFrame(msg: ByteArray, calc: YunmaiLib, user: ScaleUser) {
        if (msg.size < 11) return
        val resistance = u16be(msg, 9)
        applyExtended(resistance, calc, user)
    }

    /**
     * Compute body composition from resistance using YunmaiLib and store in accumulator.
     */
    private fun applyExtended(resistance: Int, calc: YunmaiLib, user: ScaleUser) {
        val w = acc.weight
        if (w <= 0f) {
            LogManager.d(TAG, "Weight not yet set; skipping body-composition calculation.")
            return
        }

        val fat = calc.getFat(user.age, w, resistance)
        val musclePct = calc.getMuscle(fat) / w * 100.0f
        val waterPct = calc.getWater(fat)
        val bone = calc.getBoneMass(musclePct, w)
        val lbm = calc.getLeanBodyMass(w, fat)
        val visceral = calc.getVisceralFat(fat, user.age)

        acc.fat = fat
        acc.muscle = musclePct
        acc.water = waterPct
        acc.bone = bone
        acc.lbm = lbm
        acc.visceralFat = visceral
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private fun u16be(b: ByteArray, off: Int): Int {
        if (off + 1 >= b.size) return 0
        return ((b[off].toInt() and 0xFF) shl 8) or (b[off + 1].toInt() and 0xFF)
    }

    private fun resetAccumulator() {
        acc.userId = -1
        acc.dateTime = null
        acc.weight = 0f
        acc.fat = 0f
        acc.muscle = 0f
        acc.water = 0f
        acc.bone = 0f
        acc.lbm = 0f
        acc.visceralFat = 0f
        qnResistance = 0
        qnProtocolType = 0x00
    }
}
