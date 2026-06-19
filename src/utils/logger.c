#ifdef DEBUG
#include <stdint.h>
#include <whb/log_cafe.h>
#include <whb/log_module.h>
#include <whb/log_udp.h>

uint32_t moduleLogInit = false;
uint32_t cafeLogInit   = false;
uint32_t udpLogInit    = false;
#endif // DEBUG

void initLogging() {
#ifdef DEBUG
    if (!(moduleLogInit = WHBLogModuleInit())) {
        cafeLogInit = WHBLogCafeInit();
    }
    // Always bring up UDP logging too (broadcasts to port 4405) so logs are
    // visible on the PC regardless of whether LoggingModule is installed.
    // HDMI is dead and the GamePad screen is cracked, so PC-side log capture
    // is our primary debugging channel.
    udpLogInit = WHBLogUdpInit();
#endif // DEBUG
}

void deinitLogging() {
#ifdef DEBUG
    if (moduleLogInit) {
        WHBLogModuleDeinit();
        moduleLogInit = false;
    }
    if (cafeLogInit) {
        WHBLogCafeDeinit();
        cafeLogInit = false;
    }
    if (udpLogInit) {
        WHBLogUdpDeinit();
        udpLogInit = false;
    }
#endif // DEBUG
}