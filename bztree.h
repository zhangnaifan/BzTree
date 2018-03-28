#ifndef BZTREE_H
#define BZTREE_H
#include <stdint.h>

#define META_ARRAY_SIZE		128
#define STORE_BLK_SIZE		4096
#define MERGE_BLK_SIZE		1028
#define SPLIT_BLK_SIZE		3072

struct bz_node
{
	/* PMwCAS(3), Frozen bit(1), Record Count(16), Block Size(22), Delete Size(22) */
	uint64_t status;

	/* total size of the node */
	uint32_t node_size;

	/* last index in record_meta_array in sorted keys */
	uint32_t sorted_count;

	/* PMwCAS(3), visiable(1), Offset(28), Key Length(16), Total Length(16) */
	uint64_t record_meta_array[META_ARRAY_SIZE];
	
	/* sorted keys, free space, unsorted keys */
	unsigned char store_blk[STORE_BLK_SIZE];
};

#endif // !BZTREE_H
