#ifndef PMwCAS_H
#define PMwCAS_H

#include <libpmemobj.h>
#include <libpmem.h>
#include "bzconfig.h"
#include "rel_ptr.h"
#include "gc.h"

#ifdef IS_PMEM
#define persist		pmem_persist
#else
#define persit		pmem_msync
#endif // IS_PMEM

#define RDCSS_BIT		((uint64_t)1 << 63)
#define MwCAS_BIT		((uint64_t)1 << 62)
#define DIRTY_BIT		((uint64_t)1 << 61)
#define ADDR_MASK		(((uint64_t)1 << 48) - 1)

#define ST_UNDECIDED	0
#define ST_SUCCESS		1
#define ST_FAILED		2
#define ST_FREE			3

#define EXCHANGE		InterlockedExchange
#define CAS				InterlockedCompareExchange

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
	off_t				recycle_func; /* 0：无，1：失败时释放new，2：成功时释放expect */
};

struct pmwcas_entry
{
	uint64_t			status;
	gc_entry_t			gc_entry;
	mdesc_pool_t		mdesc_pool;
	size_t				count;
	off_t				callback;
	word_entry			wdescs[WORD_DESCRIPTOR_SIZE];
};

struct pmwcas_pool
{
	//recycle_func_t		callbacks[CALLBACK_SIZE];
	gc_t *			gc;
	pmwcas_entry	mdescs[DESCRIPTOR_POOL_SIZE];
};

void pmwcas_first_use(mdesc_pool_t pool);

int pmwcas_init(mdesc_pool_t pool, PMEMoid oid);

void pmwcas_finish(mdesc_pool_t pool);

void pmwcas_recovery(mdesc_pool_t pool);

mdesc_t pmwcas_alloc(mdesc_pool_t pool, off_t recycle_policy, off_t search_pos);

void pmwcas_free(mdesc_t mdesc);

bool pmwcas_add(mdesc_t mdesc, rel_ptr<uint64_t> addr, uint64_t expect, uint64_t new_val, off_t recycle);

bool pmwcas_commit(mdesc_t mdesc);

uint64_t pmwcas_read(uint64_t * addr);

/*
在普通内存环境中实现
parent->child[0] = new node();
在NVM中，需要
1) 分配存储空间
2) 让parent->child[0]指向该存储空间的首地址
3) 持久化parent->child[0]
问题:
以上过程随时可能发生断电，造成
1) 永久性存储泄露（1或者2没有完成）
2) 指针数据污染（3没有完成）
解决方案:
1) 使用pmdk提供的原子内存分配器
2) 使用pmwcas持久化指针数据
例程:
//首先告知pmwcas我们想要修改parent->node[0]
//pmwcas返回cas新值的地址，以便存储后续申请内存的地址
tmp_ptr = pmwcas_reserve(..., &parent->node[0], ...)
//原子性的内存分配器，如果成功则tmp_ptr包含新内存的地址，否则包含空地址
POBJ_ALLOC(..., tmp_ptr, ...)
*/
template<typename T>
rel_ptr<rel_ptr<T>> pmwcas_reserve(mdesc_t mdesc, rel_ptr<rel_ptr<T>> addr, rel_ptr<T> expect, off_t recycle = 0)
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
	mdesc->wdescs[insert_point].new_val = 0;
	mdesc->wdescs[insert_point].mdesc = mdesc;
	mdesc->wdescs[insert_point].recycle_func = !recycle ? mdesc->callback : recycle;

	++mdesc->count;
	return rel_ptr<rel_ptr<T>>((rel_ptr<T>*)&mdesc->wdescs[insert_point].new_val);
}

#endif // !PMwCAS_H