#include <stdlib.h>
#include "bztree.h"
#include "bzerrno.h"
#include <iostream>
using namespace std;
/*
insert:
1) writer tests Frozen Bit, retraverse if failed
to avoid duplicate keys:
2) scan the sorted keys, fail if found a duplicate key
3) scan the unsorted keys
	3.2) fail if meet a duplicate key
	3.1) if meet a meta entry whose visiable unset and epoch = current global epoch, 
		set Retry bit and continue
4) reserve space in meta entry and block
	by adding one to record count and adding data length to node block size(both in node status)
	and flipping meta entry offset's high bit and setting the rest bits to current index global epoch
	4.1) in case fail => concurrent thread may be trying to insert duplicate key,
		so need to set Recheck flag
5) unset visiable bit and copy data to block
6) persist
6.5) re-scan prior positions if Recheck is set
	6.5.1) if find a duplicate key, set offset and block = 0
	return fail
7) 2-word PMwCAS:
	set status back => Frozen Bit isn't set
	set meta entry to visiable and correct offset
	7.1) if fails and Frozen bit is not set, read status and retry
	7.2) if Frozen bit is set, abort and retry the insert
*/

POBJ_LAYOUT_BEGIN(layout_name);
POBJ_LAYOUT_TOID(layout_name, struct bz_node);
POBJ_LAYOUT_END(layout_name);

/* 首次使用BzTree */
void bz_first_use(bz_tree * tree)
{
	pmwcas_first_use(&tree->pool);
	tree->root.set_null();
	persist(&tree->root, 64);
}

/* 初始化BzTree */
int bz_init(bz_tree * tree, PMEMobjpool * pop, PMEMoid base_oid)
{
	rel_ptr<bz_node>::set_base(base_oid);
	rel_ptr<rel_ptr<bz_node>>::set_base(base_oid);
	tree->pop = pop;
	int err = 0;
	if (err = pmwcas_init(&tree->pool, base_oid))
		return err;
	pmwcas_recovery(&tree->pool);
	return 0;
}

/* 收工 */
void bz_finish(bz_tree * tree)
{
	pmwcas_finish(&tree->pool);
}

int constr_bz_node(PMEMobjpool *pop, void *ptr, void *arg)
{
	bz_node * node = (bz_node *)ptr;
	set_node_size(node->status_, (uint64_t)arg);
	return 0;
}

int bz_alloc(bz_tree * tree, rel_ptr<rel_ptr<bz_node>> addr, size_t size) {
	mdesc_t mdesc = pmwcas_alloc(&tree->pool, 0, rand());
	auto ptr = pmwcas_reserve<bz_node>(mdesc, addr, *addr, 0);
	TOID(struct bz_node) toid;
	POBJ_ALLOC(tree->pop, &toid, struct bz_node, size, constr_bz_node, (void*)size);
	*ptr = toid.oid;
	persist(ptr.abs(), sizeof(uint64_t));
	int ret = pmwcas_commit(mdesc);
	cout << ret << endl;
	pmwcas_free(mdesc);
	return ret;
}

/* 执行叶节点的数据项插入 */
int bz_node::leaf_insert(char * key, size_t key_len, char * val, size_t val_len, uint32_t alloc_epoch) {
	/*
	1. 确保frozen位=0
	2. 2-word PMwCAS预留数据项位置: record_count/block_size, offset
	3. 拷贝数据项val, 并持久化
	4. 读状态字s, 确保frozen位=0
	5. 2-word PMwCAS: visiable/offset/key_len/total_len, s
	*/
	uint64_t status = pmwcas_read(&status_);
	if (is_frozen(status)) {
		return EFROZEN;
	}
	return 0;
}
/*
delete:
1) 2-word PMwCAS on
	meta entry: visiable = 0, offset = 0
	node status: delete size += data length
	1.1) if fails due to Frozen bit set, abort and retraverse
	1.2) otherwise, read and retry
*/

/*
update(swap pointer):
1) 3-word PMwCAS
	pointer in storage block
	meta entry status => competing delete
	node status => Frozen bit
*/

/*
read
1) binary search on sorted keys
2) linear scan on unsorted keys
3) return the record found
*/

/*
range scan: [beg_key, end_key)
one leaf node at a time
1) enter the epoch
2) construct a response_array(visiable and not deleted)
3) exit epoch
4) one-record-at-a-time
5) enter new epoch
6) greater than search the largest key in response array
*/

/*
consolidate:

trigger by
	either free space too small or deleted space too large

1) single-word PMwCAS on status => frozen bit
2) scan all valid records(visiable and not deleted)
	calculate node size = record size + free space
	2.1) if node size too large, do a split
	2.2) otherwise, allocate a new node N', copy records sorted, init header
3) use path stack to find Node's parent P
	3.1) if P's frozen, retraverse to locate P
4) 2-word PMwCAS
	r in P that points to N => N'
	P's status => detect concurrent freeze
5) N is ready for gc
6) 
*/


uint64_t * bz_node::rec_meta_arr() {
	return (uint64_t*)((char*)this + sizeof(*this));
}

/* K-V getter and setter */
char * bz_node::get_key(uint64_t meta) {
	return (char*)this + get_offset(meta);
}
void bz_node::set_key(uint64_t &meta, const char *src, size_t key_len) {
	memcpy(get_key(meta), src, key_len);
}
char * bz_node::get_value(uint64_t meta) {
	return (char*)this + get_offset(meta) + get_key_length(meta);
}
void bz_node::set_value(uint64_t &meta, const char * src, size_t value_len) {
	memcpy(get_value(meta), src, value_len);
}

/* 键值比较函数 */
template<typename Key>
int bz_node::key_cmp(uint64_t meta_1, uint64_t meta_2) {
	return *(Key*)get_key(meta_1) - *(Key*)get_key(meta_2);
}
int bz_node::key_cmp_str(uint64_t meta_1, uint64_t meta_2) {
	return strcmp(get_key(meta_1), get_key(meta_2));
}