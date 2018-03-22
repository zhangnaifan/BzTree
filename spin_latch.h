#ifndef SPIN_LATCH
#define SPIN_LATCH

#include "PMwCAS.h"

#define SPIN_UNLOCKED	0
#define SPIN_LOCKED		1
typedef uint64_t lock_t;

bool spin_try_lock(lock_t * lock)
{
	return CAS(lock, SPIN_LOCKED, SPIN_UNLOCKED);
}

void spin_lock(lock_t * lock)
{
	while (!spin_try_lock(lock))
		_mm_pause();
}

void spin_unlock(lock_t * lock)
{
	CAS(lock, SPIN_UNLOCKED, SPIN_LOCKED);
}


#endif // !SPIN_LATCH