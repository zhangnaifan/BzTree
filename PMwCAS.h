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

/* relative pointer */
template<typename T>
class rel_ptr
{
	/* base address initialized in the start */
	static UCHAR * base_address;
	/* offset from the base address */
	uint64_t off;
public:
	/* constructors */
	rel_ptr() : off(0) {}
	rel_ptr(const T * abs_ptr) : off((UCHAR*)abs_ptr - base_address) {}
	explicit rel_ptr(uint64_t abs_ptr) : off((UCHAR*)abs_ptr - base_address) {}
	rel_ptr(UCHAR * abs_ptr) : off(abs_ptr - base_address) {}
	template<typename U>
	rel_ptr(rel_ptr<U> rptr) : off((UCHAR*)rptr.abs() - base_address) {}
	rel_ptr(PMEMoid oid) : off((UCHAR*)pmemobj_direct(oid) - base_address) {}

	/* basic pointer usage */
	T& operator *() { return *(T*)(base_address + off); }
	T* operator ->() { return (T*)(base_address + off); }
	
	/* return absolute or relative address */
	uint64_t * abs() { return (uint64_t*)(base_address + off); }
	uint64_t rel() { return off; }

	/* comparison */
	bool operator ==(const rel_ptr<T>& rptr) { return off == rptr.off; }
	bool operator <(const rel_ptr<T>& rptr) { return off < rptr.off; }
	bool operator >(const rel_ptr<T>& rptr) { return off > rptr.off; }
	
	bool is_null() { return !off; }
	void set_null() { off = 0; }
	static void set_base(UCHAR* base) { base_address = base; }
	static UCHAR* null() { return base_address; }
};

struct word_entry;
struct pmwcas_entry;
struct pmwcas_pool;

typedef void(*recycle_func_t)(void*);
typedef rel_ptr<word_entry>		wdesc_t;
typedef rel_ptr<pmwcas_entry>	mdesc_t;
typedef pmwcas_pool				*mdesc_pool_t;

struct word_entry
{
	rel_ptr<uint64_t>	addr;
	uint64_t			expect;
	uint64_t			new_val;
	mdesc_t				mdesc;
	off_t				recycle_func;
};

struct pmwcas_entry
{
	uint64_t			status;
	gc_entry_t			gc_entry;
	size_t				count;
	off_t				callback;
	word_entry			wdescs[WORD_DESCRIPTOR_SIZE];
};

struct pmwcas_pool
{
	recycle_func_t		callbacks[CALLBACK_SIZE];
	pmwcas_entry		mdescs[DESCRIPTOR_POOL_SIZE];
};

void pmwcas_first_use(mdesc_pool_t pool);

void pmwcas_init(UCHAR* base);

void pmwcas_recovery(mdesc_pool_t pool);

mdesc_t pmwcas_alloc(mdesc_pool_t pool, off_t search_pos);

void pmwcas_free(mdesc_t mdesc, gc_t * gc);

void pmwcas_reclaim(gc_entry_t *entry, void *arg);

bool pmwcas_add(mdesc_t mdesc, rel_ptr<uint64_t> addr, uint64_t expect, uint64_t new_val, off_t recycle);

bool pmwcas_add(mdesc_t mdesc, rel_ptr<uint64_t> addr, rel_ptr<uint64_t> expect, rel_ptr<uint64_t> new_val, off_t recycle);

bool pmwcas_commit(mdesc_t mdesc);

uint64_t pmwcas_read(uint64_t * addr);

template<typename T>
rel_ptr<T> * pmwcas_reserve(mdesc_t mdesc, rel_ptr<uint64_t> addr, rel_ptr<uint64_t> expect, off_t recycle)
{
	off_t insert_point = (off_t)mdesc->count, i;
	wdesc_t wdesc = mdesc->wdescs;
	/* check if PMwCAS is full */
	if (mdesc->count == WORD_DESCRIPTOR_SIZE)
	{
		return false;
	}
	/*
	* check if the target address exists
	* otherwise, find the insert point
	*/
	for (i = 0; i < mdesc->count; ++i)
	{
		wdesc = mdesc->wdescs + i;
		if (wdesc->addr == addr)
		{
			return nullptr;
		}
		if (wdesc->addr > addr && insert_point > i)
		{
			insert_point = i;
		}
	}
	if (insert_point != mdesc->count)
		memmove(mdesc->wdescs + insert_point + 1,
			mdesc->wdescs + insert_point,
			(mdesc->count - insert_point) * sizeof(*mdesc->wdescs));

	mdesc->wdescs[insert_point].addr = addr;
	mdesc->wdescs[insert_point].expect = expect.rel();
	mdesc->wdescs[insert_point].mdesc = mdesc;
	mdesc->wdescs[insert_point].recycle_func = recycle;

	++mdesc->count;
	return (rel_ptr<T>*)&mdesc->wdescs[insert_point].new_val;
}

#endif // !PMwCAS_H