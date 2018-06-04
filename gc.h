#ifndef _GC_H_
#define _GC_H_

#include "ebr.h"
#define	SPINLOCK_BACKOFF_MIN	4
#define	SPINLOCK_BACKOFF_MAX	128
#if defined(__x86_64__) || defined(__i386__)
#define SPINLOCK_BACKOFF_HOOK	__asm volatile("pause" ::: "memory")
#else
#define SPINLOCK_BACKOFF_HOOK
#endif
#define	SPINLOCK_BACKOFF(count)					\
do {								\
	for (int __i = (count); __i != 0; __i--) {		\
		SPINLOCK_BACKOFF_HOOK;				\
	}							\
	if ((count) < SPINLOCK_BACKOFF_MAX)			\
		(count) += (count);				\
} while (/* CONSTCOND */ 0);


typedef struct gc_entry {
	struct gc_entry *next;
} gc_entry_t;

typedef void(*gc_func_t)(gc_entry_t *, void *);

typedef struct gc {
	/*
	* Objects are first inserted into the limbo list.  They move
	* to a current epoch list on a G/C cycle.
	*/
	gc_entry_t *	limbo;

	/*
	* A separate list for each epoch.  Objects in each list
	* are reclaimed incrementally, as ebr_sync() announces new
	* epochs ready to be reclaimed.
	*/
	gc_entry_t *	epoch_list[EBR_EPOCHS];

	/*
	* EBR object and the reclamation function.
	*/
	ebr_t *		ebr;
	off_t   	entry_off;
	gc_func_t	reclaim;
	void *		arg;
} gc_t;

gc_t *	gc_create(unsigned, gc_func_t, void *);
void	gc_destroy(gc_t *);
void	gc_register(gc_t *);

void	gc_crit_enter(gc_t *);
void	gc_crit_exit(gc_t *);

void	gc_limbo(gc_t *, void *);
void	gc_cycle(gc_t *);
void	gc_full(gc_t *, unsigned);

#endif
