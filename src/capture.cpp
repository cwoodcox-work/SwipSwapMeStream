/****************************************************************************
 * SwipSwapMeStream - TV frame capture / live JPEG streamer (Phase 3)
 *
 * Pipeline (each stage on its own thread so they overlap):
 *   [GX2 hook]  de-tile TV color buffer -> reused LINEAR surface   (~1.4ms)
 *        | sRawPending + sMutex/sCond
 *   [encoder]   coordinator forks N strip workers (one per core),
 *               each unpacks+JPEG-encodes its horizontal strip in
 *               parallel; coordinator joins + assembles the payload
 *        | sSendPending + sSendMutex/sSendCond (1-deep, latest-wins)
 *   [sender]    write the multi-strip frame to the socket           (~28ms)
 *
 * A single-thread encode was the wall (~40ms unpack+encode => ~22fps). The
 * Espresso has 3 cores, so each frame is split into STREAM_ENCODE_STRIPS
 * horizontal strips encoded in parallel by persistent core-pinned workers.
 * Frames are dropped at every handoff if the next stage is busy, so nothing
 * ever backs up (live, low-latency).
 *
 * Protocol v2 handshake (console -> PC, once per connection, before any frame):
 *   [4 bytes] magic "SSMS"
 *   [1 byte ] STREAM_PROTOCOL_VERSION
 *
 * Multi-strip wire protocol (console -> PC), repeated per frame:
 *   [4 bytes BE] outerLen (number of bytes that follow)
 *   [2 bytes BE] full frame width W
 *   [2 bytes BE] full frame height H
 *   [1 byte    ] strip count N
 *   then N times:
 *     [2 bytes BE] strip Y offset (row in the full frame)
 *     [2 bytes BE] strip height
 *     [1 byte    ] strip type (STRIP_TYPE_UNCHANGED | STRIP_TYPE_JPEG)
 *     if STRIP_TYPE_JPEG:
 *       [4 bytes BE] jpeg length
 *       [jpeg bytes] a complete baseline JPEG (W x stripHeight)
 *
 * Dirty-strip skipping: each strip's source pixels are FNV-1a hashed; a strip
 * whose hash matches the previous frame is sent as STRIP_TYPE_UNCHANGED (no
 * JPEG, no encode work). The first frame of every connection and the first
 * frame after any resolution change are forced full (all strips JPEG) so the
 * viewer always has a complete baseline.
 *
 * The console LISTENS on CAPTURE_TCP_PORT (INADDR_ANY, no hardcoded IP); a PC
 * viewer connects in. Capture only happens while a viewer is connected.
 ****************************************************************************/
#include "capture.h"
#include "thirdparty/toojpeg.h"
#include "utils/logger.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <coreinit/systeminfo.h>
#include <coreinit/thread.h>
#include <coreinit/time.h>
#include <gx2/enum.h>
#include <gx2/event.h>
#include <gx2/mem.h>
#include <gx2/state.h>
#include <gx2/surface.h>
#include <memory/mappedmemory.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <nn/ac.h>
#include <nn/result.h>
#include <sys/socket.h>
#include <unistd.h>

// GX2 surface formats we know how to unpack.
#define GX2_FMT_R10G10B10A2 0x19u
#define GX2_FMT_RGBA8       0x1au

namespace {

    // WHBLogUdp is not thread-safe and several of our threads log; serialize all
    // of this plugin's logging behind one mutex so concurrent log calls can't
    // corrupt the logger (which previously hung the console). __LINE__/__FUNCTION__
    // still resolve to the call site since these are macros.
    std::mutex sLogMutex;
#define STREAM_LOG_INFO(...)                         \
    do {                                             \
        std::lock_guard<std::mutex> _llk(sLogMutex); \
        DEBUG_FUNCTION_LINE_INFO(__VA_ARGS__);       \
    } while (0)
#define STREAM_LOG_WARN(...)                         \
    do {                                             \
        std::lock_guard<std::mutex> _llk(sLogMutex); \
        DEBUG_FUNCTION_LINE_WARN(__VA_ARGS__);       \
    } while (0)
#define STREAM_LOG_ERR(...)                          \
    do {                                             \
        std::lock_guard<std::mutex> _llk(sLogMutex); \
        DEBUG_FUNCTION_LINE_ERR(__VA_ARGS__);        \
    } while (0)


    // Clamp the configured strip count to [1, 3] (the Espresso has 3 cores).
    constexpr int kNumStrips = STREAM_ENCODE_STRIPS < 1 ? 1
                                                        : (STREAM_ENCODE_STRIPS > 3 ? 3 : STREAM_ENCODE_STRIPS);
    // Core assignment, most-isolated-from-the-game first: cores 0 and 2 leave
    // core 1 (the main game core) freest; a 3rd strip then takes core 1.
    constexpr int kStripCore[3] = {0, 2, 1};

    // --- Adaptive quality: bands select resolution (downscale) + the quality
    // range the auto-controller works within. The viewer picks a band; the
    // console auto-tunes quality inside it to the highest the wifi sustains. ---
    struct Band {
        uint32_t downscale; // 1280x720 / downscale
        uint32_t qMin;      // adaptive quality floor for this band
        uint32_t qMax;      // adaptive quality ceiling for this band
    };
    constexpr Band kBands[3] = {
            {3, 25, 50}, // 0 Performance: 426x240
            {2, 30, 60}, // 1 Balanced:    640x360
            {2, 55, 80}, // 2 Quality:     640x360
    };
    constexpr uint32_t kQualityHardMin = 15;
    constexpr uint32_t kQualityHardMax = 90;

    std::atomic<uint32_t> sMode{STREAM_DEFAULT_MODE}; // 0/1/2 index into kBands
    std::atomic<bool> sAuto{true};                    // adaptive quality enabled
    std::atomic<uint32_t> sQuality{40};               // current JPEG quality
    std::atomic<uint32_t> sDownscale{2};              // current downscale factor

    std::thread sServerThread;
    std::atomic<bool> sServerRunning{false};
    std::atomic<bool> sStreaming{false};  // a viewer is connected and streaming
    std::atomic<bool> sRawPending{false}; // hook has a frame waiting in sLinearSurface
    std::atomic<int> sListenFd{-1};
    uint32_t sFrameCounter = 0;

    // hook -> encoder handoff
    std::mutex sMutex;
    std::condition_variable sCond;

    // encoder -> sender handoff (1-deep, latest wins)
    std::mutex sSendMutex;
    std::condition_variable sSendCond;
    std::vector<uint8_t> sSendSlot; // full multi-strip payload incl. outerLen prefix
    bool sSendPending = false;

    // De-tile destination, reused across frames (lives in GX2 mapped memory).
    GX2Surface sLinearSurface{};
    bool sLinearValid = false;

    // Captured frame metadata (written by hook under sMutex, read by encoder).
    uint32_t sMetaWidth      = 0;
    uint32_t sMetaHeight     = 0;
    uint32_t sMetaPitch      = 0;
    uint32_t sMetaFormat     = 0;
    OSTime sCaptureTimeStamp = 0; // GPU timestamp the de-tile copy completes at

    // Strip workers run BELOW the game's threads (which sit around prio 16) so the
    // OS scheduler always gives the game CPU first and we encode on spare cycles.
    // This stops the encoder from stealing cores 0/2 from the game (0=highest,
    // 31=lowest priority on the Wii U).
    constexpr int kWorkerPriority = 24;

    // Encoder-thread-only reusable buffer for the assembled payload.
    std::vector<uint8_t> sPayload;

    // --- Instrumentation (per-second summary over the UDP log) ---
    std::atomic<uint32_t> sHookGx2Us{0};    // total GX2 de-tile time (us) this window
    std::atomic<uint32_t> sHookCaptured{0}; // frames de-tiled by the hook this window
    std::atomic<uint32_t> sStatSendUs{0};   // total send time (us) this window
    std::atomic<uint32_t> sStatSent{0};     // frames sent this window

    // toojpeg uses a plain function-pointer callback (no captures/context). With
    // several workers encoding at once, each must write to its own buffer.
    // thread_local can't be used here (the WUPS relocation fixer rejects TLS
    // relocs), so we give each worker slot its own sink pointer + callback; a
    // worker only ever uses the callback for its own index, so no clobbering.
    std::vector<uint8_t> *sJpegSink[3] = {nullptr, nullptr, nullptr};
    void jpegWrite0(unsigned char b) { sJpegSink[0]->push_back(b); }
    void jpegWrite1(unsigned char b) { sJpegSink[1]->push_back(b); }
    void jpegWrite2(unsigned char b) { sJpegSink[2]->push_back(b); }
    TooJpeg::WRITE_ONE_BYTE jpegSinkFn(int idx) {
        switch (idx) {
            case 0:
                return jpegWrite0;
            case 1:
                return jpegWrite1;
            default:
                return jpegWrite2;
        }
    }

    // --- Strip worker pool (fork/join, persistent, core-pinned) ---
    struct StripWorker {
        std::thread thread;
        int idx    = 0;
        uint32_t y = 0;            // first row of this strip in the downscaled frame
        uint32_t h = 0;            // number of rows in this strip
        std::vector<uint8_t> rgb;  // unpacked RGB888 for this strip
        std::vector<uint8_t> jpeg; // encoded JPEG for this strip
        bool ok           = false;
        uint64_t hash     = 0;     // FNV-1a of this frame's source pixels
        uint64_t lastHash = 0;     // hash of the last JPEG-encoded frame (for skip)
        bool unchanged    = false; // this strip matched lastHash -> send UNCHANGED
    };
    StripWorker sWorkers[3];

    std::mutex sWorkMutex;
    std::condition_variable sWorkStartCond;
    std::condition_variable sWorkDoneCond;
    uint32_t sWorkGen  = 0; // bumped to dispatch a new frame to the workers
    uint32_t sWorkDone = 0; // workers finished with the current generation
    bool sWorkersQuit  = false;

    // Frame params shared with the workers (stable while they run).
    uint32_t sStripOutW       = 0;
    uint32_t sStripScale      = 1;
    uint32_t sStripFormat     = 0;
    uint32_t sStripPitchBytes = 0;
    uint32_t sStripQuality    = 40; // JPEG quality for this frame (all strips equal)
    const uint8_t *sStripBase = nullptr;
    bool sStripForceKey       = false; // this frame: encode every strip (ignore hash)

    // Set true so the next captured frame is a full keyframe (all strips JPEG):
    // on each new viewer connection and on any resolution change. Read+cleared by
    // the encoder thread.
    std::atomic<bool> sForceKeyframe{true};
    // Encoder-thread-only: the encoded dimensions of the previous frame, to detect
    // a resolution change (which invalidates every strip's stored hash).
    uint32_t sLastOutW = 0;
    uint32_t sLastOutH = 0;

    // Apply a mode (band): set resolution + clamp current quality into the band.
    void applyMode(uint32_t mode) {
        if (mode > 2) {
            return;
        }
        sMode.store(mode);
        sDownscale.store(kBands[mode].downscale);
        uint32_t q = sQuality.load();
        if (q < kBands[mode].qMin) q = kBands[mode].qMin;
        if (q > kBands[mode].qMax) q = kBands[mode].qMax;
        sQuality.store(q);
    }

    void putBE16(std::vector<uint8_t> &v, uint32_t x) {
        v.push_back(static_cast<uint8_t>((x >> 8) & 0xFF));
        v.push_back(static_cast<uint8_t>(x & 0xFF));
    }
    void putBE32(std::vector<uint8_t> &v, uint32_t x) {
        v.push_back(static_cast<uint8_t>((x >> 24) & 0xFF));
        v.push_back(static_cast<uint8_t>((x >> 16) & 0xFF));
        v.push_back(static_cast<uint8_t>((x >> 8) & 0xFF));
        v.push_back(static_cast<uint8_t>(x & 0xFF));
    }

    bool sendAll(int fd, const void *buf, size_t len) {
        auto *p     = static_cast<const uint8_t *>(buf);
        size_t sent = 0;
        while (sent < len) {
            const ssize_t n = send(fd, p + sent, len - sent, 0);
            if (n <= 0) {
                return false;
            }
            sent += static_cast<size_t>(n);
        }
        return true;
    }

    void freeLinearSurface() {
        if (sLinearSurface.image != nullptr) {
            MEMFreeToMappedMemory(sLinearSurface.image);
        }
        sLinearSurface       = GX2Surface{};
        sLinearSurface.image = nullptr;
        sLinearValid         = false;
    }

    // Ensure sLinearSurface is a linear copy-target matching the source surface.
    // Reuses the existing allocation if the geometry/format is unchanged.
    bool ensureLinearSurface(const GX2Surface &src) {
        if (sLinearValid && sLinearSurface.width == src.width &&
            sLinearSurface.height == src.height && sLinearSurface.format == src.format) {
            return true;
        }
        freeLinearSurface();

        sLinearSurface           = src;
        sLinearSurface.tileMode  = GX2_TILE_MODE_LINEAR_ALIGNED;
        sLinearSurface.mipLevels = 1;
        sLinearSurface.swizzle   = 0;
        sLinearSurface.image     = nullptr;
        sLinearSurface.mipmaps   = nullptr;
        GX2CalcSurfaceSizeAndAlignment(&sLinearSurface);

        sLinearSurface.image = MEMAllocFromMappedMemoryForGX2Ex(sLinearSurface.imageSize, sLinearSurface.alignment);
        if (sLinearSurface.image == nullptr) {
            STREAM_LOG_ERR("Stream: failed to alloc %u bytes for linear surface", sLinearSurface.imageSize);
            sLinearValid = false;
            return false;
        }
        sLinearValid = true;
        return true;
    }

    // FNV-1a hash of the source pixels this strip samples (the same strided set
    // of 32-bit pixels unpackStrip would read). Hashing the SOURCE lets us detect
    // an unchanged strip and skip both the unpack and the (expensive) JPEG encode.
    // One FNV step per pixel (4 bytes folded into a u32), so it is far cheaper
    // than the encode it can save. 64-bit math is fine here (it is plain data, not
    // a std::atomic, which the 32-bit Espresso cannot do at 64 bits).
    uint64_t hashStrip(const StripWorker &w) {
        const uint32_t scale      = sStripScale;
        const uint32_t outW       = sStripOutW;
        const uint32_t pitchBytes = sStripPitchBytes;
        const uint32_t stepBytes  = scale * 4;
        const uint8_t *base       = sStripBase;

        uint64_t hash = 1469598103934665603ull; // FNV-1a offset basis
        for (uint32_t row = 0; row < w.h; row++) {
            const uint8_t *px = base + static_cast<size_t>((w.y + row) * scale) * pitchBytes;
            for (uint32_t ox = 0; ox < outW; ox++) {
                const uint32_t p = static_cast<uint32_t>(px[0]) |
                                   (static_cast<uint32_t>(px[1]) << 8) |
                                   (static_cast<uint32_t>(px[2]) << 16) |
                                   (static_cast<uint32_t>(px[3]) << 24);
                hash ^= p;
                hash *= 1099511628211ull; // FNV prime
                px += stepBytes;
            }
        }
        return hash;
    }

    // Unpack + downscale one worker's horizontal strip into its RGB888 buffer.
    // Bytes are read explicitly (little-endian compose) so this is correct on
    // the big-endian Espresso; matches the confirmed Phase-2 decode.
    void unpackStrip(StripWorker &w) {
        const uint32_t scale      = sStripScale;
        const uint32_t outW       = sStripOutW;
        const uint32_t pitchBytes = sStripPitchBytes;
        const uint32_t stepBytes  = scale * 4;
        const uint8_t *base       = sStripBase;

        w.rgb.resize(static_cast<size_t>(outW) * w.h * 3);
        uint8_t *dst = w.rgb.data();

        if (sStripFormat == GX2_FMT_R10G10B10A2) {
            for (uint32_t row = 0; row < w.h; row++) {
                const uint32_t oy = w.y + row;
                const uint8_t *px = base + static_cast<size_t>(oy * scale) * pitchBytes;
                for (uint32_t ox = 0; ox < outW; ox++) {
                    const uint32_t p = static_cast<uint32_t>(px[0]) |
                                       (static_cast<uint32_t>(px[1]) << 8) |
                                       (static_cast<uint32_t>(px[2]) << 16) |
                                       (static_cast<uint32_t>(px[3]) << 24);
                    dst[0] = static_cast<uint8_t>((p & 0x3FF) >> 2);
                    dst[1] = static_cast<uint8_t>(((p >> 10) & 0x3FF) >> 2);
                    dst[2] = static_cast<uint8_t>(((p >> 20) & 0x3FF) >> 2);
                    dst += 3;
                    px += stepBytes;
                }
            }
        } else { // GX2_FMT_RGBA8: bytes are R,G,B,A in memory order
            for (uint32_t row = 0; row < w.h; row++) {
                const uint32_t oy = w.y + row;
                const uint8_t *px = base + static_cast<size_t>(oy * scale) * pitchBytes;
                for (uint32_t ox = 0; ox < outW; ox++) {
                    dst[0] = px[0];
                    dst[1] = px[1];
                    dst[2] = px[2];
                    dst += 3;
                    px += stepBytes;
                }
            }
        }
    }

    // Persistent worker: pins itself to a core, then loops encoding one strip per
    // dispatched generation until asked to quit.
    void stripWorkerLoop(int idx) {
        OSSetThreadAffinity(OSGetCurrentThread(), 1u << kStripCore[idx]);
        OSSetThreadPriority(OSGetCurrentThread(), kWorkerPriority); // below the game

        uint32_t myGen = 0;
        while (true) {
            {
                std::unique_lock<std::mutex> lk(sWorkMutex);
                sWorkStartCond.wait(lk, [&] { return sWorkGen != myGen || sWorkersQuit; });
                if (sWorkersQuit) {
                    return;
                }
                myGen = sWorkGen;
            }

            StripWorker &w = sWorkers[idx];
            w.hash         = hashStrip(w);
            if (!sStripForceKey && w.hash == w.lastHash) {
                // Source pixels are identical to the last frame we sent for this
                // strip: emit UNCHANGED, skipping unpack + encode entirely.
                w.unchanged = true;
                w.ok        = true;
            } else {
                w.unchanged = false;
                unpackStrip(w);
                w.jpeg.clear();
                w.ok = TooJpeg::writeJpeg(jpegSinkFn(idx), w.rgb.data(),
                                          static_cast<unsigned short>(sStripOutW),
                                          static_cast<unsigned short>(w.h),
                                          true, static_cast<unsigned char>(sStripQuality), STREAM_JPEG_420);
                // Only commit the hash once the strip actually encoded, so a failed
                // encode is retried (not skipped) on the next frame.
                if (w.ok) {
                    w.lastHash = w.hash;
                }
            }

            {
                std::lock_guard<std::mutex> lk(sWorkMutex);
                sWorkDone++;
            }
            sWorkDoneCond.notify_one();
        }
    }

    void startWorkers() {
        sWorkersQuit = false;
        sWorkGen     = 0;
        sWorkDone    = 0;
        for (int i = 0; i < kNumStrips; i++) {
            sWorkers[i].idx    = i;
            sJpegSink[i]       = &sWorkers[i].jpeg; // bind this slot's callback target
            sWorkers[i].thread = std::thread(stripWorkerLoop, i);
        }
    }

    void stopWorkers() {
        {
            std::lock_guard<std::mutex> lk(sWorkMutex);
            sWorkersQuit = true;
        }
        sWorkStartCond.notify_all();
        for (int i = 0; i < kNumStrips; i++) {
            if (sWorkers[i].thread.joinable()) {
                sWorkers[i].thread.join();
            }
            sWorkers[i].rgb.clear();
            sWorkers[i].rgb.shrink_to_fit();
            sWorkers[i].jpeg.clear();
            sWorkers[i].jpeg.shrink_to_fit();
        }
    }

    // Sender thread: drains the 1-deep send slot and writes frames to the socket,
    // overlapping with the encoder. On send failure it flags the stream as down
    // so the encoder loop also exits.
    void senderThread(int clientFd) {
        std::vector<uint8_t> local;
        while (sServerRunning && sStreaming) {
            {
                std::unique_lock<std::mutex> lk(sSendMutex);
                sSendCond.wait_for(lk, std::chrono::milliseconds(200),
                                   [] { return sSendPending || !sStreaming || !sServerRunning; });
                if (!sStreaming || !sServerRunning) {
                    break;
                }
                if (!sSendPending) {
                    continue;
                }
                local.swap(sSendSlot);
                sSendPending = false;
            }

            const OSTime t0 = OSGetSystemTime();
            const bool ok   = sendAll(clientFd, local.data(), local.size());
            sStatSendUs.fetch_add(static_cast<uint32_t>(OSTicksToMicroseconds(OSGetSystemTime() - t0)));
            sStatSent.fetch_add(1);
            if (!ok) {
                STREAM_LOG_INFO("Stream: viewer disconnected (send)");
                sStreaming.store(false);
                sCond.notify_all();
                break;
            }
        }
    }

    // Encoder/coordinator loop (runs on the server thread while a viewer is
    // connected): waits for a de-tiled frame, forks the strip workers, joins
    // them, assembles the multi-strip payload, and hands it to the sender.
    void encoderLoop() {
        OSTime windowStart      = OSGetSystemTime();
        uint32_t statEnc        = 0;
        uint64_t statParallelUs = 0, statBytes = 0, statWaitUs = 0;
        uint32_t statW = 0, statH = 0;
        uint32_t statDropped = 0; // frames overwritten unsent (network behind)
        uint32_t statSkipped = 0; // UNCHANGED strips this window (dirty-skip wins)
        uint32_t statStrips  = 0; // total strips this window (= statEnc * kNumStrips)

        while (sServerRunning && sStreaming) {
            {
                std::unique_lock<std::mutex> lk(sMutex);
                sCond.wait_for(lk, std::chrono::milliseconds(200),
                               [] { return sRawPending.load() || !sStreaming || !sServerRunning; });
                if (!sServerRunning || !sStreaming) {
                    break;
                }
                if (!sRawPending.load()) {
                    continue;
                }
                // sMeta* + sLinearSurface are stable while sRawPending == true.
            }

            // Wait for the GPU to finish the de-tile copy (this blocks the ENCODER
            // thread, not the game's render thread), then make the result visible
            // to the CPU. Decoupling this off the game thread is what keeps the
            // game's framerate independent of our capture.
            const OSTime tWait0 = OSGetSystemTime();
            GX2WaitTimeStamp(sCaptureTimeStamp);
            GX2Invalidate(GX2_INVALIDATE_MODE_CPU, sLinearSurface.image, sLinearSurface.imageSize);
            statWaitUs += OSTicksToMicroseconds(OSGetSystemTime() - tWait0);

            const OSTime tWork0 = OSGetSystemTime();

            uint32_t scale = sDownscale.load();
            if (scale < 1) scale = 1;
            const uint32_t outW = sMetaWidth / scale;
            const uint32_t outH = sMetaHeight / scale;

            // Decide whether this frame must be a full keyframe (every strip JPEG):
            // the first frame of a connection (sForceKeyframe) or any resolution
            // change, which moves every strip boundary and invalidates the hashes.
            bool forceKey = sForceKeyframe.exchange(false);
            if (outW != sLastOutW || outH != sLastOutH) {
                forceKey = true;
            }
            sLastOutW = outW;
            sLastOutH = outH;

            // Dispatch: set the per-strip params, then bump the generation.
            {
                std::lock_guard<std::mutex> lk(sWorkMutex);
                sStripOutW       = outW;
                sStripScale      = scale;
                sStripFormat     = sMetaFormat;
                sStripPitchBytes = sMetaPitch * 4;
                sStripQuality    = sQuality.load();
                sStripBase       = static_cast<const uint8_t *>(sLinearSurface.image);
                sStripForceKey   = forceKey;

                const uint32_t stripRows = outH / kNumStrips;
                uint32_t y               = 0;
                for (int i = 0; i < kNumStrips; i++) {
                    sWorkers[i].y = y;
                    sWorkers[i].h = (i == kNumStrips - 1) ? (outH - y) : stripRows;
                    y += stripRows;
                }
                sWorkDone = 0;
                sWorkGen++;
            }
            sWorkStartCond.notify_all();

            // Join: wait for every strip to finish reading the surface + encoding.
            {
                std::unique_lock<std::mutex> lk(sWorkMutex);
                sWorkDoneCond.wait(lk, [] { return sWorkDone == static_cast<uint32_t>(kNumStrips); });
            }

            // Workers are done reading sLinearSurface; let the hook capture again.
            sRawPending.store(false);

            const OSTime tWork1 = OSGetSystemTime();

            // Assemble the multi-strip payload (reserve 4 bytes for outerLen). A
            // strip is fine if it was skipped (UNCHANGED) or it encoded a non-empty
            // JPEG; a changed strip with a failed/empty encode aborts the frame.
            bool allOk = true;
            for (int i = 0; i < kNumStrips; i++) {
                if (!sWorkers[i].unchanged && (!sWorkers[i].ok || sWorkers[i].jpeg.empty())) {
                    allOk = false;
                    break;
                }
            }
            if (!allOk) {
                STREAM_LOG_WARN("Stream: a strip JPEG encode failed");
                // Some strips may have committed their hash this frame; force the
                // next frame full so no strip is skipped without a fresh baseline.
                sForceKeyframe.store(true);
                continue;
            }

            sPayload.clear();
            sPayload.resize(4); // outerLen placeholder
            putBE16(sPayload, outW);
            putBE16(sPayload, outH);
            sPayload.push_back(static_cast<uint8_t>(kNumStrips));
            uint32_t frameBytes   = 0;
            uint32_t frameSkipped = 0;
            for (int i = 0; i < kNumStrips; i++) {
                const StripWorker &w = sWorkers[i];
                putBE16(sPayload, w.y);
                putBE16(sPayload, w.h);
                if (w.unchanged) {
                    sPayload.push_back(STRIP_TYPE_UNCHANGED);
                    frameSkipped++;
                } else {
                    sPayload.push_back(STRIP_TYPE_JPEG);
                    putBE32(sPayload, static_cast<uint32_t>(w.jpeg.size()));
                    sPayload.insert(sPayload.end(), w.jpeg.begin(), w.jpeg.end());
                    frameBytes += static_cast<uint32_t>(w.jpeg.size());
                }
            }
            const uint32_t outerLen = static_cast<uint32_t>(sPayload.size() - 4);
            const uint32_t lenBE    = htonl(outerLen);
            memcpy(sPayload.data(), &lenBE, 4);

            // Hand off to the sender (latest wins: overwrite an unsent frame).
            // An overwrite means the sender hadn't drained the previous frame =
            // the wifi link is behind; count it as the adaptive backpressure signal.
            {
                std::lock_guard<std::mutex> lk(sSendMutex);
                if (sSendPending) {
                    statDropped++;
                }
                sSendSlot.swap(sPayload);
                sSendPending = true;
            }
            sSendCond.notify_one();

            statEnc++;
            statParallelUs += OSTicksToMicroseconds(tWork1 - tWork0);
            statBytes += frameBytes;
            statSkipped += frameSkipped;
            statStrips += static_cast<uint32_t>(kNumStrips);
            statW = outW;
            statH = outH;

            const uint64_t elapsedUs = OSTicksToMicroseconds(tWork1 - windowStart);
            if (elapsedUs >= 1000000 && statEnc > 0) {
                const uint32_t captured = sHookCaptured.exchange(0);
                const uint32_t gx2Us    = sHookGx2Us.exchange(0);
                const uint32_t sent     = sStatSent.exchange(0);
                const uint32_t sendUs   = sStatSendUs.exchange(0);

                // --- Adaptive quality (AIMD within the current band) ---
                // Drop rate is the saturation signal: back off fast when the wifi
                // can't keep up, probe up slowly when it can. Held within the band
                // the viewer selected; skipped entirely when auto is off.
                const uint32_t mode = sMode.load();
                const uint32_t qMin = kBands[mode].qMin;
                const uint32_t qMax = kBands[mode].qMax;
                if (sAuto.load()) {
                    uint32_t q = sQuality.load();
                    if (statDropped * 100 > statEnc * 15 && q > qMin) {
                        q = (q >= qMin + 5) ? q - 5 : qMin; // saturated: drop fast
                    } else if (statDropped * 100 < statEnc * 3 && q < qMax) {
                        q = (q + 3 <= qMax) ? q + 3 : qMax; // headroom: probe up
                    }
                    sQuality.store(q);
                }

                STREAM_LOG_INFO(
                        "Stream stats: %ux%u x%d strips | enc %u fps, sent %u fps, drop %u | skip %u/%u (%u%%) | q=%u %s(%u-%u) | unpack+encode %llu us | gpuwait %llu us | send %u us | %llu KB/frame | gx2enq %u us x%u/s",
                        statW, statH, kNumStrips, statEnc, sent, statDropped,
                        statSkipped, statStrips, statStrips ? statSkipped * 100 / statStrips : 0,
                        sQuality.load(), sAuto.load() ? "auto " : "PIN ", qMin, qMax,
                        statParallelUs / statEnc,
                        statWaitUs / statEnc,
                        sent ? sendUs / sent : 0,
                        (statBytes / statEnc) / 1024,
                        captured ? gx2Us / captured : 0, captured);
                windowStart    = tWork1;
                statEnc        = 0;
                statParallelUs = statBytes = statWaitUs = 0;
                statDropped                             = 0;
                statSkipped = statStrips = 0;
            }
        }
    }

    // Nudge the pinned quality by a step, disabling auto (manual override).
    void nudgeQuality(int delta) {
        sAuto.store(false);
        int q = static_cast<int>(sQuality.load()) + delta;
        if (q < static_cast<int>(kQualityHardMin)) q = kQualityHardMin;
        if (q > static_cast<int>(kQualityHardMax)) q = kQualityHardMax;
        sQuality.store(static_cast<uint32_t>(q));
    }

    // Control reader: receives 1-byte commands from the viewer over the same TCP
    // connection (full-duplex) and applies mode / auto / quality overrides.
    void controlReaderThread(int clientFd) {
        uint8_t b;
        while (sServerRunning && sStreaming) {
            const ssize_t n = recv(clientFd, &b, 1, 0);
            if (n <= 0) {
                // Viewer closed (or half-closed) the connection; tear down.
                sStreaming.store(false);
                sCond.notify_all();
                sSendCond.notify_all();
                break;
            }
            const uint32_t prevMode = sMode.load();
            const bool prevAuto     = sAuto.load();
            const uint32_t prevQ    = sQuality.load();
            switch (b) {
                case CTRL_MODE_PERFORMANCE:
                    applyMode(0);
                    break;
                case CTRL_MODE_BALANCED:
                    applyMode(1);
                    break;
                case CTRL_MODE_QUALITY:
                    applyMode(2);
                    break;
                case CTRL_AUTO_ON:
                    sAuto.store(true);
                    break;
                case CTRL_AUTO_OFF:
                    sAuto.store(false);
                    break;
                case CTRL_QUALITY_UP:
                    nudgeQuality(+5);
                    break;
                case CTRL_QUALITY_DOWN:
                    nudgeQuality(-5);
                    break;
                default:
                    continue;
            }
            // Only log on an actual state change, so holding/spamming a key (e.g.
            // quality already at the cap) can't flood the logger.
            if (sMode.load() != prevMode || sAuto.load() != prevAuto ||
                sQuality.load() != prevQ) {
                STREAM_LOG_INFO("Stream: control 0x%02X -> mode %u, %s, q=%u",
                                b, sMode.load(), sAuto.load() ? "auto" : "pin",
                                sQuality.load());
            }
        }
    }

    void logConsoleAddress() {
        uint32_t ip = 0;
        if (NNResult_IsSuccess(ACGetAssignedAddress(&ip)) && ip != 0) {
            DEBUG_FUNCTION_LINE_INFO("Stream server listening on %u.%u.%u.%u:%d",
                                     (ip >> 24) & 0xFF, (ip >> 16) & 0xFF,
                                     (ip >> 8) & 0xFF, ip & 0xFF, CAPTURE_TCP_PORT);
        } else {
            DEBUG_FUNCTION_LINE_INFO("Stream server listening on port %d (console IP unknown)",
                                     CAPTURE_TCP_PORT);
        }
    }

    void serverThread() {
        const int listenFd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (listenFd < 0) {
            DEBUG_FUNCTION_LINE_ERR("Stream: socket() failed");
            sServerRunning = false;
            return;
        }
        sListenFd = listenFd;

        int reuse = 1;
        setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port        = htons(CAPTURE_TCP_PORT);

        if (bind(listenFd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0) {
            DEBUG_FUNCTION_LINE_ERR("Stream: bind() failed on port %d", CAPTURE_TCP_PORT);
            close(listenFd);
            sListenFd      = -1;
            sServerRunning = false;
            return;
        }

        listen(listenFd, 1);
        logConsoleAddress();

        // The strip workers live for the whole server lifetime (pinned once).
        startWorkers();

        while (sServerRunning) {
            const int clientFd = accept(listenFd, nullptr, nullptr);
            if (clientFd < 0) {
                if (!sServerRunning) {
                    break;
                }
                continue;
            }

            int nodelay = 1;
            setsockopt(clientFd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
            // Deliberately SMALL send buffer (~1.5 frames). When wifi falls behind,
            // send() blocks almost immediately instead of absorbing a backlog of
            // frames (bufferbloat -> latency). The blocked sender lets the encoder's
            // latest-wins slot drop stale frames, so only fresh frames go out and
            // glass-to-glass latency stays bounded even when we overproduce.
            int sndbuf = 64 * 1024;
            setsockopt(clientFd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

            // Protocol handshake: "SSMS" + version, so the viewer can refuse an
            // incompatible console rather than misparse the first frame's bytes.
            const uint8_t handshake[5] = {'S', 'S', 'M', 'S', STREAM_PROTOCOL_VERSION};
            if (!sendAll(clientFd, handshake, sizeof(handshake))) {
                STREAM_LOG_WARN("Stream: handshake send failed; dropping viewer");
                close(clientFd);
                continue;
            }

            sFrameCounter = 0;
            sRawPending.store(false);
            // First frame for this viewer must be a full keyframe (all strips JPEG)
            // so they start from a complete baseline; also reset the res tracker.
            sForceKeyframe.store(true);
            sLastOutW = 0;
            sLastOutH = 0;
            {
                std::lock_guard<std::mutex> lk(sSendMutex);
                sSendPending = false;
            }
            // Reset adaptive state to the default band for each new viewer.
            sAuto.store(true);
            applyMode(STREAM_DEFAULT_MODE);
            sStreaming.store(true);

            DEBUG_FUNCTION_LINE_INFO("Stream: viewer connected");
            std::thread sender(senderThread, clientFd);
            std::thread control(controlReaderThread, clientFd);
            encoderLoop();

            // Tear down this connection. shutdown() wakes the control reader (and
            // the sender) blocked on the socket so they can be joined.
            sStreaming.store(false);
            sCond.notify_all();
            sSendCond.notify_all();
            shutdown(clientFd, SHUT_RDWR);
            if (sender.joinable()) {
                sender.join();
            }
            if (control.joinable()) {
                control.join();
            }
            sRawPending.store(false);
            close(clientFd);
        }

        stopWorkers();
        close(listenFd);
        sListenFd = -1;
    }

} // namespace

void StartCaptureServer() {
    if (sServerRunning.exchange(true)) {
        return; // already running
    }
    sStreaming    = false;
    sRawPending   = false;
    sServerThread = std::thread(serverThread);
}

void StopCaptureServer() {
    if (!sServerRunning.exchange(false)) {
        return; // not running
    }
    sStreaming = false;
    sCond.notify_all();
    sSendCond.notify_all();

    const int fd = sListenFd.exchange(-1);
    if (fd >= 0) {
        shutdown(fd, SHUT_RDWR);
        close(fd);
    }
    if (sServerThread.joinable()) {
        sServerThread.join();
    }

    sRawPending = false;
    freeLinearSurface();
    sPayload.clear();
    sPayload.shrink_to_fit();
    sSendSlot.clear();
    sSendSlot.shrink_to_fit();
}

void CaptureTVFrameIfRequested(GX2ColorBuffer *colorBuffer) {
    if (!sStreaming.load()) {
        return; // no viewer connected
    }
    if ((sFrameCounter++ % STREAM_FRAME_SKIP) != 0) {
        return; // frame-skip
    }
    if (sRawPending.load()) {
        return; // encoder still busy with the previous frame -> drop this one
    }

    const uint32_t format = colorBuffer->surface.format;
    if (format != GX2_FMT_R10G10B10A2 && format != GX2_FMT_RGBA8) {
        static bool warned = false;
        if (!warned) {
            warned = true;
            STREAM_LOG_WARN("Stream: unsupported TV format 0x%X; not streaming", format);
        }
        return;
    }

    if (!ensureLinearSurface(colorBuffer->surface)) {
        return;
    }

    // Enqueue the de-tile copy on the GPU and flush it, but DO NOT wait here:
    // GX2DrawDone() on the game's render thread would drain the GPU every frame
    // and wreck the game's pacing. Instead grab the GPU timestamp the copy will
    // retire at; the encoder thread waits on it (off the game thread).
    const OSTime gx2Start = OSGetSystemTime();
    GX2CopySurface(&colorBuffer->surface, 0, 0, &sLinearSurface, 0, 0);
    GX2Flush(); // submit the copy to the GPU (async; does not block)
    const OSTime ts = GX2GetLastSubmittedTimeStamp();
    sHookGx2Us.fetch_add(static_cast<uint32_t>(OSTicksToMicroseconds(OSGetSystemTime() - gx2Start)));
    sHookCaptured.fetch_add(1);

    {
        std::lock_guard<std::mutex> lk(sMutex);
        sMetaWidth        = sLinearSurface.width;
        sMetaHeight       = sLinearSurface.height;
        sMetaPitch        = sLinearSurface.pitch;
        sMetaFormat       = sLinearSurface.format;
        sCaptureTimeStamp = ts;
        sRawPending.store(true);
    }
    sCond.notify_one();
}
