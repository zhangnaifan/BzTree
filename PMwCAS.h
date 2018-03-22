#ifndef PMwCAS_H
#define PMwCAS_H

#include <libpmemobj.h>
#include <libpmem.h>
#include "gc.h"

#define IS_PMEM		1

#ifdef IS_PMEM
#define persist		pmem_persist
#else
#define persit		pmem_msync
#endif // IS_PMEM

#define DESCRIPTOR_POOL_SIZE	64
#define WORD_DESCRIPTOR_SIZE	4
#define CALLBACK_SIZE			4

#define RDCSS_BIT		((uint64_t)1 << 48)
#define MwCAS_BIT		((uint64_t)1 << 49)
#define DIRTY_BIT		((uint64_t)1 << 50)
#define ADDR_MASK		(((uint64_t)1 << 48) - 1)

#define ST_UNDECIDED	0
#define ST_SUCCESS		1
#define ST_FAILED		2
#define ST_FREE			3

#define VALID			0
#define INVALID			1

#define EXCHANGE		InterlockedExchange
#define CAS				InterlockedCompareExchange

typedef void(*recycle_func_t)(void*);
typedef struct word_entry * wdesc_t;
typedef struct PMwCAS_entry * mdesc_t;
typedef struct descriptor_pool * mdesc_pool_t;

struct word_entry
{
	uint64_t			*addr;
	uint64_t			expect;
	uint64_t			new_val;
	mdesc_t				mdesc;
	off_t				recycle_func;
};

struct PMwCAS_entry
{
	uint64_t			status;
	gc_entry_t			gc_entry;
	size_t				count;
	off_t				callback;
	word_entry			wdescs[WORD_DESCRIPTOR_SIZE];
};

struct descriptor_pool
{
	recycle_func_t		callbacks[CALLBACK_SIZE];
	PMwCAS_entry		mdescs[DESCRIPTOR_POOL_SIZE];
};

void init_pool(mdesc_pool_t pool);

void recovery_PMwCAS(mdesc_pool_t pool);

mdesc_t alloc_PMwCAS(mdesc_pool_t pool, off_t search_pos);

void free_PMwCAS(mdesc_t mdesc, gc_t * gc);

void reclaim_PMwCAS(gc_entry_t *entry, void *arg);

bool add_entry(PMEMobjpool *pop, mdesc_t mdesc, uint64_t * addr, uint64_t expect, uint64_t new_val, off_t recycle);

bool PMwCAS(mdesc_t mdesc);

uint64_t pread(uint64_t * addr);

#endif // !PMwCAS
