/****************************************************************************
 * SwipSwapMeStream - TV frame capture / live JPEG streamer (protocol v2)
 *
 * The console LISTENS on CAPTURE_TCP_PORT (INADDR_ANY - no IP is hardcoded).
 * When a PC viewer connects, the console streams live JPEG frames until the
 * viewer disconnects.
 *
 * Protocol v2 adds (a) a one-time handshake so the viewer can refuse an
 * incompatible console rather than misparse, and (b) per-strip dirty-region
 * skipping: a strip whose source pixels are byte-identical to the previous
 * frame is sent as a 1-byte UNCHANGED marker instead of a re-encoded JPEG, so
 * static regions (HUDs, menus, paused games) cost almost nothing on the wire.
 *
 * Handshake (console -> PC, once per connection, before any frame):
 *   [4 bytes] magic "SSMS"
 *   [1 byte ] protocol version (STREAM_PROTOCOL_VERSION)
 *
 * Stream wire protocol (console -> PC), repeated per frame (see capture.cpp for
 * the multi-strip payload layout). Dimensions are carried in the payload, so the
 * console can change resolution mid-stream and the viewer adapts.
 *
 * Adaptive quality: the console auto-tunes JPEG quality to the highest the wifi
 * link sustains (AIMD on dropped frames), within the band selected by the
 * current MODE. The PC viewer can override via 1-byte control commands sent back
 * up the same TCP connection (see CTRL_* below).
 *
 * Capture is opt-in: nothing is captured/encoded unless a viewer is connected.
 * The GX2 hook only de-tiles + copies on every Nth frame, and only when the
 * encoder is idle (frames are dropped aggressively so the game never waits).
 ****************************************************************************/
#pragma once

#include <gx2/surface.h>

#define CAPTURE_TCP_PORT        7766

// --- Protocol handshake (console -> PC, once per connection) ---
// 4-byte magic "SSMS" then this version byte. The viewer refuses/warns on a
// mismatch instead of misparsing an old or foreign stream. Bump on any wire
// change; keep in sync with pc-viewer/src/main.rs.
#define STREAM_PROTOCOL_VERSION 2

// --- Per-strip type byte (protocol v2) ---
// In each per-frame payload, every strip carries a type byte after its
// [Y][height]. UNCHANGED carries no further payload (the viewer keeps its
// previous pixels for that strip); JPEG is followed by [4 BE len][jpeg].
#define STRIP_TYPE_UNCHANGED    0x00
#define STRIP_TYPE_JPEG         0x01

// --- Tunables (sane defaults; edit here). ---
// Capture 1 out of every STREAM_FRAME_SKIP TV frames (1 => uncapped, up to 60;
// the encoder/wifi + adaptive quality become the real cap. 2 => ~30 fps).
#define STREAM_FRAME_SKIP       1
// Use YCbCr 4:2:0 chroma subsampling (smaller frames, minor quality loss).
#define STREAM_JPEG_420         true
// Number of horizontal strips encoded in parallel across the Espresso's cores.
// 2 => cores 0 and 2 (leaves core 1, the main game core, freer). 3 => all cores
// (faster, but more contention with the game). Max 3.
#define STREAM_ENCODE_STRIPS    2
// Default mode at connect: 0 = Performance, 1 = Balanced, 2 = Quality.
#define STREAM_DEFAULT_MODE     1

// --- Control protocol (PC viewer -> console), single bytes over the TCP link ---
#define CTRL_MODE_PERFORMANCE   0x01 // low res + low quality band (max framerate)
#define CTRL_MODE_BALANCED      0x02 // medium res + medium quality band
#define CTRL_MODE_QUALITY       0x03 // medium res + high quality band
#define CTRL_AUTO_ON            0x10 // re-enable adaptive quality
#define CTRL_AUTO_OFF           0x11 // pin current quality (disable adapt)
#define CTRL_QUALITY_UP         0x20 // nudge quality up (also pins / auto off)
#define CTRL_QUALITY_DOWN       0x21 // nudge quality down (also pins / auto off)

#ifdef __cplusplus
extern "C" {
#endif

// Start/stop the background TCP streaming server. Safe to call from
// ON_APPLICATION_START / ON_APPLICATION_REQUESTS_EXIT.
void StartCaptureServer();
void StopCaptureServer();

// Called from the GX2 TV-scan hook. If a viewer is connected and the encoder
// is idle, de-tiles this frame into a reusable linear surface and hands it to
// the streamer thread. Cheap no-op otherwise.
void CaptureTVFrameIfRequested(GX2ColorBuffer *colorBuffer);

#ifdef __cplusplus
}
#endif
