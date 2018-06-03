#ifndef BZCONFIG_H
#define BZCONFIG_H

#define IS_PMEM		
#define BZ_DEBUG

#define DESCRIPTOR_POOL_SIZE	512
#define WORD_DESCRIPTOR_SIZE	4
#define CALLBACK_SIZE			4

#define GC_WAIT_MS				10

#ifdef BZ_DEBUG
//数据格式为<Key = uint64_t, Val = rel_ptr<uint64_t>>
#define NODE_MIN_FREE_SIZE		sizeof(uint64_t) * 3 * 1		// <= 1 consolidate
#define NODE_MAX_DELETE_SIZE	sizeof(uint64_t) * 2			// >= 2 delete
#define NODE_SPLIT_SIZE			sizeof(uint64_t) * 2 * 6		// 6 split 
#define NODE_MERGE_SIZE			sizeof(uint64_t) * 3 * 3 - 1	// < 3 merge
#define NODE_ALLOC_SIZE			sizeof(uint64_t) * 3 * 7		// max 6

#else

#define NODE_CONSOLIDATE_SIZE	1024
#define NODE_SPLIT_SIZE			4096
#define NODE_MERGE_SIZE			2048
#define NODE_ALLOC_SIZE			5120

#endif // BZ_DEBUG

#endif // !BZCONFIG_H
