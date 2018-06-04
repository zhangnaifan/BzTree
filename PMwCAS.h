#ifndef PMwCAS_H
#define PMwCAS_H

#include <libpmemobj.h>
#include <libpmem.h>
#include "bzconfig.h"
#include "rel_ptr.h"
#include "gc.h"
#include "utils.h"

#ifdef IS_PMEM
#define persist		pmem_persist
#else
#define persist		pmem_msync
#endif // IS_PMEM

#define RDCSS_BIT		0x8000000000000000
#define MwCAS_BIT		0x4000000000000000
#define DIRTY_BIT		0x2000000000000000
#define ADDR_MASK		0xffffffffffff

#define ST_UNDECIDED	0
#define ST_SUCCESS		1
#define ST_FAILED		2
#define ST_FREE			3

#define RELEASE_NEW_ON_FAILED	1
#define RELEASE_EXP_ON_SUCCESS	2
#define RELEASE_SWAP_PTR		3 //release new on failed and release expect on success
#define RELEASE_ADDR_ON_SUCCESS	4 //only for temporily store memory ptr

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
	uint64_t		magic[WORD_DESCRIPTOR_SIZE];
	bz_memory_pool  mem_;
};

/* 
* 第一次使用pmwcas时调用
* 初始化描述符状态
*/
void pmwcas_first_use(mdesc_pool_t pool, PMEMobjpool * pop, PMEMoid oid);

/*
* 每次重启时首先调用
* 设置相对指针的基地址
* 创建G/C和回收线程
*/
int pmwcas_init(mdesc_pool_t pool, PMEMoid oid, PMEMobjpool * pop);

/*
* 结束时调用
* GC
* 销毁G/C
*/
void pmwcas_finish(mdesc_pool_t pool);

/*
* 每次重启时调用
* 修复描述符状态
* 完成或回滚上次未完成的PMwCAS
* 回收可能泄漏的内存区域
*/
void pmwcas_recovery(mdesc_pool_t pool);

/*
* 每次执行PMwCAS之前调用
* 确保描述符内目标地址互不相同
* 返回PMwCAS描述符的相对指针
* 失败时返回空的相对指针
*/
mdesc_t pmwcas_alloc(mdesc_pool_t pool, off_t recycle_policy = 0, off_t search_pos = 0);

/*
* 放弃执行PMwCAS, 单线程调用
* 用于暂时保管分配的内存，以便于系统断电后回收
*/
bool pmwcas_abort(mdesc_t mdesc);
rel_ptr<uint64_t> get_magic(mdesc_pool_t pool, int i);

/*
* 每次执行完PMwCAS之后调用
* 回收对应的PMwCAS描述符
*/
void pmwcas_free(mdesc_t mdesc);

/* 执行释放函数 */
void pmwcas_word_recycle(mdesc_pool_t pool, rel_ptr<rel_ptr<uint64_t>> ptr_leak);
/*
* 向PMwCAS描述符添加一个CAS字段
* 返回成功/失败
*/
bool pmwcas_add(mdesc_t mdesc, rel_ptr<uint64_t> addr, uint64_t expect, uint64_t new_val, off_t recycle = 0);

/* 执行PMwCAS */
bool pmwcas_commit(mdesc_t mdesc);

/* 读取可能被PMwCAS操作的字的值 */
uint64_t pmwcas_read(uint64_t * addr);

/*
* 向PMwCAS描述符添加一个CAS字段
* 但是new_val域留空，用于填写后续分配新内存的地址
* recycle字段用于指定某些条件下的NVM内存回收
* 包括0：无，1：失败时回收new_val，2：成功时回收expect
*/
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