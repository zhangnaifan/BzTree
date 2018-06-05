#ifndef BZCONFIG_H
#define BZCONFIG_H

#define IS_PMEM		
//#define BZ_DEBUG

#define PRE_ALLOC_NUM			128
#define MAX_ALLOC_NUM			1024

#define DESCRIPTOR_POOL_SIZE	4096
#define WORD_DESCRIPTOR_SIZE	10

#define GC_THREADS_COUNT		10
#define GC_WAIT_MS				10

#ifdef BZ_TEST
//数据格式为<Key = uint64_t, Val = rel_ptr<uint64_t>>
#define NODE_MIN_FREE_SIZE		sizeof(uint64_t) * 3 * 1		// <= 1 consolidate
#define NODE_MAX_DELETE_SIZE	sizeof(uint64_t) * 2			// >= 2 delete
#define NODE_SPLIT_SIZE			sizeof(uint64_t) * 2 * 6		// 6 split 
#define NODE_MERGE_SIZE			sizeof(uint64_t) * 3 * 3 - 1	// < 3 merge
#define NODE_ALLOC_SIZE			sizeof(uint64_t) * 3 * 7		// max 6

#else

#define NODE_MIN_FREE_SIZE		512
#define NODE_MAX_DELETE_SIZE	1024
#define NODE_SPLIT_SIZE			4096
#define NODE_MERGE_SIZE			2048
#define NODE_ALLOC_SIZE			5120

#endif // BZ_DEBUG

#endif // !BZCONFIG_H
