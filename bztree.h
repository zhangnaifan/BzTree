#ifndef BZTREE_H
#define BZTREE_H
#include "PMwCAS.h"
using namespace std;

template<typename Key, typename Val>
struct bz_tree;

/* BzTree节点头部 */
template<typename Key, typename Val>
struct bz_node
{
	/* 3: PMwCAS control, 1: frozen, 16: record count, 22: block size, 22: delete size */
	uint64_t status_;
	/* 32: node size, 32: sorted count */
	uint64_t length_;
	/* record meta entry */
	/* 3: PMwCAS control, 1: visiable, 28: offset, 16: key length, 16: total length */
	
	uint64_t * rec_meta_arr();
	/* K-V getter and setter */
	Key * get_key(uint64_t meta);
	void set_key(uint32_t offset, const Key * key);
	Val * get_value(uint64_t meta);
	void set_value(uint32_t offset, const Val * val);
	/* 键值比较函数 */
	int key_cmp(uint64_t meta_1, const Key * key);
	/* 执行叶节点的数据项插入 */
	int insert(bz_tree<Key, Val> * tree, Key * key, Val * val, uint32_t key_size, uint32_t total_size, uint32_t alloc_epoch);
	int binary_search(uint64_t * meta_arr, int size, const Key * key);
};

/* BzTree */
template<typename Key, typename Val>
struct bz_tree {
	PMEMobjpool *		pop_;
	pmwcas_pool			pool_;
	rel_ptr<bz_node<Key, Val>>	root_;
	
	void first_use();
	int init(PMEMobjpool * pop, PMEMoid base_oid);
	void finish();
	int alloc_node(rel_ptr<rel_ptr<bz_node<Key, Val>>> addr, rel_ptr<bz_node<Key, Val>> expect, size_t size);
};









/* 位操作函数集 BEGIN */
inline bool is_frozen(uint64_t status) {
	return 1 & (status >> 60);
}
inline void set_frozen(uint64_t &s) {
	s |= ((uint64_t)1 << 60);
}
inline uint32_t get_record_count(uint64_t status) {
	return 0xffff & (status >> 44);
}
inline void set_record_count(uint64_t &s, uint64_t new_record_count) {
	s = (s & 0xf0000fffffffffff) | (new_record_count << 44);
}
inline uint32_t get_block_size(uint64_t status) {
	return 0x3fffff & (status >> 22);
}
inline void set_block_size(uint64_t &s, uint64_t new_block_size) {
	s = (s & 0xfffff000003fffff) | (new_block_size << 22);
}
inline uint32_t get_delete_size(uint64_t status) {
	return 0x3fffff & status;
}
inline void set_delete_size(uint64_t &s, uint32_t new_delete_size) {
	s = (s & 0xffffffffffc0000) | new_delete_size;
}
inline uint32_t get_node_size(uint64_t length) {
	return length >> 32;
}
inline void set_node_size(uint64_t &s, uint64_t new_node_size) {
	s = (s & 0xffffffff) | (new_node_size << 32);
}
inline uint32_t get_sorted_count(uint64_t length) {
	return length & 0xffffffff;
}
inline void set_sorted_count(uint64_t &s, uint32_t new_sorted_count) {
	s = (s & 0xffffffff00000000) | new_sorted_count;
}
inline bool is_visiable(uint64_t meta) {
	return !(1 & (meta >> 60));
}
inline void set_visiable(uint64_t &meta) {
	meta &= 0xefffffffffffffff;
}
inline void unset_visiable(uint64_t &meta) {
	meta |= ((uint64_t)1 << 60);
}
inline uint32_t get_offset(uint64_t meta) {
	return 0xfffffff & (meta >> 32);
}
inline void set_offset(uint64_t &meta, uint64_t new_offset) {
	meta = (meta & 0xf0000000ffffffff) | (new_offset << 32);
}
inline uint16_t get_key_length(uint64_t meta) {
	return 0xffff & (meta >> 16);
}
inline void set_key_length(uint64_t &meta, uint32_t new_key_length) {
	meta = (meta & 0xffffffff0000ffff) | (new_key_length << 16);
}
inline uint32_t get_total_length(uint64_t meta) {
	return 0xffff & meta;
}
inline void set_total_length(uint64_t &meta, uint16_t new_total_length) {
	meta = (meta & 0xffffffffffff0000) | new_total_length;
}
/* 位操作函数集合 END */

#endif // !BZTREE_H
