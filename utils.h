#ifndef UTILS_H
#define UTILS_H

#include <windows.h>

#define BTS_acquire InterlockedBitTestAndSet64Acquire
#define BTR_release InterlockedBitTestAndReset64Release

#define CAS			InterlockedCompareExchange
#define EXCHANGE	InterlockedExchange
#endif // !UTILS_H
