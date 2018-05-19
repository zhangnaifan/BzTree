#include <thread>
#include <vector>
#include <tuple>

#include "bztree.h"
#include "bzerrno.h"

struct bz_node_fake {};
POBJ_LAYOUT_BEGIN(layout_name);
POBJ_LAYOUT_TOID(layout_name, struct bz_node_fake);
POBJ_LAYOUT_END(layout_name);

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
/* 执行叶节点的数据项插入 */
template<typename Key, typename Val>
int bz_node<Key, Val>::insert(bz_tree<Key, Val> * tree, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size, uint32_t alloc_epoch)
{
	/* Global variables */
	uint64_t * meta_arr = rec_meta_arr();
	uint32_t node_sz = get_node_size(length_);
	uint32_t sorted_cnt = get_sorted_count(length_);
	bool recheck = false;
	uint64_t meta_new;
	uint32_t rec_cnt, blk_sz;

	/* UNIKEY 查找有序部分是否有重复键值 */
	uint32_t pos;
	if (find_key_sorted(key, pos))
		return EUNIKEY;

	uint64_t status_rd = pmwcas_read(&status_);
	if (is_frozen(status_rd))
		return EFROZEN;
	/* UNIKEY 查找无序部分是否有重复键值 */
	if (find_key_unsorted(key, status_rd, alloc_epoch, pos, recheck))
		return EUNIKEY;

	while (true)
	{
		status_rd = pmwcas_read(&status_);
		if (is_frozen(status_rd))
			return EFROZEN;
		rec_cnt = get_record_count(status_rd);
		blk_sz = get_block_size(status_rd);

		/* 可容纳 */
		if (blk_sz + total_size + sizeof(uint64_t) * (1 + rec_cnt) + sizeof(*this) > node_sz)
			return EALLOCSIZE;

		/* 增加record count和block size */
		uint64_t status_new = status_add_rec_blk(status_rd, total_size);

		/* 设置offset为alloc_epoch, unset visiable bit */
		uint64_t meta_rd = pmwcas_read(&meta_arr[rec_cnt]);
		meta_new = meta_vis_off(meta_rd, false, alloc_epoch);

		/* 2-word PMwCAS */
		if (pack_pmwcas(&tree->pool_, {
			{ &status_, status_rd, status_new },
			{ &meta_arr[rec_cnt], meta_rd, meta_new }
			}))
			break;
		recheck = true;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}

	/* copy key and val */
	uint32_t new_offset = node_sz - blk_sz - total_size - 1;
	copy_data(new_offset, key, val, key_size, total_size);

	if (recheck) {
		int ret = rescan_unsorted(sorted_cnt, rec_cnt, key, total_size, alloc_epoch);
		if (ret)
			return ret;
	}

	/* set visiable; real offset; key_len and tot_len */
	uint64_t meta_new_plus = meta_vis_off_klen_tlen(meta_new, true, new_offset, key_size, total_size);

	while (true)
	{
		status_rd = pmwcas_read(&status_);
		if (is_frozen(status_rd)) {
			set_offset(meta_arr[rec_cnt], 0);
			persist(&meta_arr[rec_cnt], sizeof(uint64_t));
			return EFROZEN;
		}
		/* 2-word PMwCAS */
		if (pack_pmwcas(&tree->pool_, {
			{ &status_, status_rd, status_rd },
			{ &meta_arr[rec_cnt], meta_new, meta_new_plus }
			}))
			break;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
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
/* 叶节点删除 */
template<typename Key, typename Val>
int bz_node<Key, Val>::remove(bz_tree<Key, Val> * tree, const Key * key)
{
	/* Global variables */
	uint64_t * meta_arr = rec_meta_arr();
	uint32_t pos;
	bool found;

	/* check Frozen */
	uint64_t status_rd = pmwcas_read(&status_);
	uint32_t rec_cnt = get_record_count(status_rd);
	if (is_frozen(status_rd))
		return EFROZEN;

	/* 二分查找有序键值部分 */
	found = find_key_sorted(key, pos);

	/* 查找无序部分是否有key */
	if (!found) {
		bool useless;
		found = find_key_unsorted(key, status_rd, 0, pos, useless);
	}

	if (found) {
		while (true)
		{
			status_rd = pmwcas_read(&status_);
			if (is_frozen(status_rd))
				return EFROZEN;

			uint64_t meta_rd = pmwcas_read(&meta_arr[pos]);
			if (!is_visiable(meta_rd)) {
				/* 遭遇其他线程的竞争删除 */
				return ENOTFOUND;
			}

			/* 增加delete size */
			uint64_t status_new = status_del(status_rd, get_total_length(meta_rd));

			/* unset visible，offset=0*/
			uint64_t meta_new = meta_vis_off(meta_rd, false, 0);

			/* 2-word PMwCAS */
			if (pack_pmwcas(&tree->pool_, {
				{ &status_, status_rd, status_new },
				{ &meta_arr[pos], meta_rd, meta_new }
				}))
				break;
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		return 0;
	}
	return ENOTFOUND;
}
/*
update
1) writer tests Frozen Bit, retraverse if failed
to find the target key:
2) scan the sorted keys, store the positon if find the target key
2.1) if not found, scan the unsorted keys, fail if cannot find, otherwise store the pos
2.2) if meet a meta entry whose visiable unset and epoch = current global epoch,
set Retry bit and continue
3.1) reserve space in meta entry and block
by adding one to record count and adding data length to node block size(both in node status)
and setting the offset to current index global epoch and unset visual
3.2) in case fail => concurrent thread may be trying to insert duplicate key,
so need to set Recheck flag. and keep reading status, and perform pmwcas

4) copy data to block
5) persist
6) re-scan the area (key_pos, rec_cnt) if Recheck is set
6.1) if find a duplicate key, set offset and block = 0
return fail
7) 3-word PMwCAS:
set status back => Frozen Bit isn't set
set updated meta entry to visiable and correct offset
set origin meta entry to unvisualable and offset = 0
in case fail:
7.1) Frozen set => return fail
7.2) origin meta entry not visual => return fail
otherwise, read origin meta and status, retry pmwcas
*/
/* 叶节点数据更新 */
template<typename Key, typename Val>
int bz_node<Key, Val>::update(bz_tree<Key, Val> * tree, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size, uint32_t alloc_epoch)
{
	/* Global variables */
	uint64_t * meta_arr = rec_meta_arr();
	uint32_t node_sz = get_node_size(length_);
	uint32_t sorted_cnt = get_sorted_count(length_);
	bool recheck = false;
	uint64_t meta_new;
	uint32_t rec_cnt, blk_sz;

	/* not Frozen */
	uint64_t status_rd = pmwcas_read(&status_);
	if (is_frozen(status_rd))
		return EFROZEN;

	/* 查找目标键值的位置 */
	uint32_t del_pos;
	if (!find_key_sorted(key, del_pos) 
		&& !find_key_unsorted(key, status_rd, alloc_epoch, del_pos, recheck))
		return ENOTFOUND;
	
	while (true)
	{
		status_rd = pmwcas_read(&status_);
		if (is_frozen(status_rd))
			return EFROZEN;
		rec_cnt = get_record_count(status_rd);
		blk_sz = get_block_size(status_rd);

		/* 可容纳 */
		if (blk_sz + total_size + sizeof(uint64_t) * (1 + rec_cnt) + sizeof(*this) > node_sz)
			return EALLOCSIZE;

		/* 增加record count和block size */
		uint64_t status_new = status_add_rec_blk(status_rd, total_size);

		/* 设置offset为alloc_epoch, unset visiable bit */
		uint64_t meta_rd = pmwcas_read(&meta_arr[rec_cnt]);
		meta_new = meta_vis_off(meta_rd, false, alloc_epoch);

		/* 2-word PMwCAS */
		if (pack_pmwcas(&tree->pool_, {
			{ &status_, status_rd, status_new },
			{ &meta_arr[rec_cnt], meta_rd, meta_new }
			}))
			break;
		recheck = true;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}

	/* copy key and val */
	uint32_t new_offset = node_sz - blk_sz - total_size - 1;
	copy_data(new_offset, key, val, key_size, total_size);

	if (recheck) {
		int ret = rescan_unsorted(del_pos + 1, rec_cnt, key, total_size, alloc_epoch);
		if (ret)
			return ret;
	}

	/* set visiable; real offset; key_len and tot_len */
	uint64_t meta_new_plus = meta_vis_off_klen_tlen(meta_new, true, new_offset, key_size, total_size);

	while (true)
	{
		status_rd = pmwcas_read(&status_);
		if (is_frozen(status_rd)) {
			/* frozen set */
			set_offset(meta_arr[rec_cnt], 0);
			persist(&meta_arr[rec_cnt], sizeof(uint64_t));
			return EFROZEN;
		}
		uint64_t meta_del = pmwcas_read(&meta_arr[del_pos]);
		if (!is_visiable(meta_del)) {
			uint64_t status_new;
			/* 遭遇竞争删除或更新，放弃保留的空间 */
			set_offset(meta_arr[rec_cnt], 0);
			persist(&meta_arr[rec_cnt], sizeof(uint64_t));
			do {
				status_rd = pmwcas_read(&status_);
				status_new = status_del(status_rd, total_size);
			} while (!is_frozen(status_rd)
				&& CAS(&status_, status_new, status_rd) != status_rd);
			if (is_frozen(status_rd))
				return EFROZEN;
			return EUNIKEY;
		}
		uint64_t status_new = status_del(status_rd, get_total_length(meta_del));
		uint64_t meta_del_new = meta_vis_off(meta_del, false, 0);
		/* 3-word PMwCAS */
		if (pack_pmwcas(&tree->pool_, {
			{ &status_, status_rd, status_new },
			{ &meta_arr[rec_cnt], meta_new, meta_new_plus },
			{ &meta_arr[del_pos], meta_del, meta_del_new }
			}))
			break;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}

	return 0;
}

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

/* 首次使用BzTree */
template<typename Key, typename Val>
void bz_tree<Key, Val>::first_use()
{
	pmwcas_first_use(&pool_);
	root_.set_null();
	persist(&root_, sizeof(uint64_t));
}

/* 初始化BzTree */
template<typename Key, typename Val>
int bz_tree<Key, Val>::init(PMEMobjpool * pop, PMEMoid base_oid)
{
	rel_ptr<bz_node<Key, Val>>::set_base(base_oid);
	rel_ptr<rel_ptr<bz_node<Key, Val>>>::set_base(base_oid);
	pop_ = pop;
	int err = 0;
	if (err = pmwcas_init(&pool_, base_oid))
		return err;
	pmwcas_recovery(&pool_);
	return 0;
}

/* 收工 */
template<typename Key, typename Val>
void bz_tree<Key, Val>::finish()
{
	pmwcas_finish(&pool_);
}

template<typename Key, typename Val>
int bz_tree<Key, Val>::alloc_node(rel_ptr<rel_ptr<bz_node<Key, Val>>> addr, rel_ptr<bz_node<Key, Val>> expect, size_t size) {
	mdesc_t mdesc = pmwcas_alloc(&pool_);
	if (mdesc.is_null())
		return EPMWCASALLOC;
	rel_ptr<rel_ptr<bz_node<Key, Val>>> ptr = pmwcas_reserve<bz_node<Key, Val>>(mdesc, addr, expect);
	TX_BEGIN(pop_) {
		pmemobj_tx_add_range_direct(ptr.abs(), sizeof(uint64_t));
		*ptr = pmemobj_tx_alloc(size, TOID_TYPE_NUM(struct bz_node_fake));
	} TX_END;
	memset((*ptr).abs(), 0, size);
	set_node_size((*ptr)->length_, size);
	persist((*ptr).abs(), size);
	bool ret = pmwcas_commit(mdesc);
	pmwcas_free(mdesc);
	return ret ? 0 : EPMWCASFAIL;
}

/*
* 在键值有序存储部分查找 @param key
* 返回结果bool 、位置 @param pos
*/
template<typename Key, typename Val>
bool bz_node<Key, Val>::find_key_sorted(const Key * key, uint32_t &pos)
{
	uint64_t * meta_arr = rec_meta_arr();
	uint32_t sorted_cnt = get_sorted_count(length_);
	pos = binary_search(meta_arr, sorted_cnt, key);
	if (pos != sorted_cnt) {
		uint64_t meta_rd = pmwcas_read(&meta_arr[pos]);
		if (is_visiable(meta_rd) && !key_cmp(meta_rd, key))
			return true;
	}
	return false;
}
/*
* 在键值无序存储部分线性查找 @param key
* @param rec_cnt: 最大边界位置
* @param alloc_epoch: recheck的判断条件
* 返回结果bool
* 返回位置 @param pos
* 返回重试标志 @param recheck
*/
template<typename Key, typename Val>
bool bz_node<Key, Val>::find_key_unsorted(const Key * key, uint64_t status_rd, uint32_t alloc_epoch, uint32_t &pos, bool &retry)
{
	uint64_t * meta_arr = rec_meta_arr();
	uint32_t sorted_cnt = get_sorted_count(length_);
	uint32_t rec_cnt = get_record_count(status_rd);
	for (pos = sorted_cnt; pos < rec_cnt; ++pos)
	{
		uint64_t meta_rd = pmwcas_read(&meta_arr[pos]);
		if (is_visiable(meta_rd)) {
			if (!key_cmp(meta_rd, key))
				return true;
		}
		else if (!retry && get_offset(meta_rd) == alloc_epoch)
			retry = true;
	}
	return false;
}
/*
* 为给定状态字@param status_rd 的record_count++
* block_size加上 @param total_size
* 返回新状态字uint64_t
*/
template<typename Key, typename Val>
uint64_t bz_node<Key, Val>::status_add_rec_blk(uint64_t status_rd, uint32_t total_size)
{
	uint32_t rec_cnt = get_record_count(status_rd);
	uint32_t blk_sz = get_block_size(status_rd);
	set_record_count(status_rd, rec_cnt + 1);
	set_block_size(status_rd, blk_sz + total_size);
	return status_rd;
}
/*
* 为给定meta_entry @param meta_rd 的delete-size加上数据项大小
* 返回新meta_entry uint64_t
*/
template<typename Key, typename Val>
uint64_t bz_node<Key, Val>::status_del(uint64_t status_rd, uint32_t total_size)
{
	uint32_t dele_sz = get_delete_size(status_rd);
	set_delete_size(status_rd, dele_sz + total_size);
	return status_rd;
}
/*
* 设置meta_entry @param meta_rd
* @param set_vis 是否可见
* @param new_offset 新的offset
* 返回新meta_entry uint64_t
*/
template<typename Key, typename Val>
uint64_t bz_node<Key, Val>::meta_vis_off(uint64_t meta_rd, bool set_vis, uint32_t new_offset)
{
	if (set_vis)
		set_visiable(meta_rd);
	else
		unset_visiable(meta_rd);
	set_offset(meta_rd, new_offset);
	return meta_rd;
}
/*
* 设置meta_entry @param meta_rd
* @param set_vis 是否可见
* @param new_offset 新的offset
* @param key_size 键值长度
* @param total_size 总长度
* 返回新meta_entry uint64_t
*/
template<typename Key, typename Val>
uint64_t bz_node<Key, Val>::meta_vis_off_klen_tlen(uint64_t meta_rd, bool set_vis, uint32_t new_offset, uint32_t key_size, uint32_t total_size)
{
	if (set_vis)
		set_visiable(meta_rd);
	else
		unset_visiable(meta_rd);
	set_offset(meta_rd, new_offset);
	set_key_length(meta_rd, key_size);
	set_total_length(meta_rd, total_size);
	return meta_rd;
}
/*
* 拷贝数据项的key value到block storage
* @param new_offset: 新数据项到节点头的偏移
*/
template<typename Key, typename Val>
void bz_node<Key, Val>::copy_data(uint32_t new_offset, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size)
{
	set_key(new_offset, key);
	set_value(new_offset + key_size, val);
	persist((char *)this + new_offset, total_size);
}
/*
* 重新扫描无序键值区域 */
template<typename Key, typename Val>
int bz_node<Key, Val>::rescan_unsorted(uint32_t beg_pos, uint32_t rec_cnt, const Key * key, uint32_t total_size, uint32_t alloc_epoch)
{
	uint64_t * meta_arr = rec_meta_arr();
	uint32_t i = beg_pos;
	while (i < rec_cnt)
	{
		uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
		if (is_visiable(meta_rd)) {
			if (!key_cmp(meta_rd, key)) {
				set_offset(meta_arr[rec_cnt], 0);
				persist(&meta_arr[rec_cnt], sizeof(uint64_t));
				uint64_t status_rd, status_new;
				do {
					status_rd = pmwcas_read(&status_);
					status_new = status_del(status_rd, total_size);
				} while (!is_frozen(status_rd)
					&& CAS(&status_, status_new, status_rd) != status_rd);
				if (is_frozen(status_rd))
					return EFROZEN;
				return EUNIKEY;
			}
		}
		else if (get_offset(meta_rd) == alloc_epoch) {
			// 潜在的UNIKEY竞争，必须等待其完成
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			continue;
		}
		++i;
	}
	return 0;
}

/*
* 返回key的插入位置（相等时取最左位置）
* 空洞情况下，左右探测找到非空洞元素
*/
template<typename Key, typename Val>
int bz_node<Key, Val>::binary_search(uint64_t * meta_arr, int size, const Key * key)
{
	int left = -1, right = size;
	while (left + 1 < right) {
		int mid = left + (right - left) / 2;
		uint64_t meta_rd = pmwcas_read(&meta_arr[mid]);
		/* 空洞处理 BEGIN */
		if (!is_visiable(meta_rd)) {
			bool go_left = true;
			int left_mid = mid - 1, right_mid = mid + 1;
			while (left_mid > left || right_mid < right) {
				if (go_left && left_mid > left || right_mid == right) {
					meta_rd = pmwcas_read(&meta_arr[left_mid]);
					if (is_visiable(meta_rd)) {
						mid = left_mid;
						break;
					}
					go_left = false;
					--left_mid;
				}
				if (!go_left && right_mid < right || left_mid == left) {
					meta_rd = pmwcas_read(&meta_arr[right_mid]);
					if (is_visiable(meta_rd)) {
						mid = right_mid;
						break;
					}
					go_left = true;
					++right_mid;
				}
			}
			if (left_mid == left && right_mid == right) {
				return right;
			}
		}
		/* 空洞处理 END */

		if (key_cmp(meta_rd, key) < 0)
			left = mid;
		else
			right = mid;
	}
	return right;
}
/* 封装pmwcas的使用 */
template<typename Key, typename Val>
bool bz_node<Key, Val>::pack_pmwcas(mdesc_pool_t pool, std::vector<std::tuple<rel_ptr<uint64_t>, uint64_t, uint64_t>> casn)
{
	mdesc_t mdesc = pmwcas_alloc(pool);
	if (mdesc.is_null())
		return false;
	for (auto cas : casn)
		pmwcas_add(mdesc, std::get<0>(cas), std::get<1>(cas), std::get<2>(cas));
	bool done = pmwcas_commit(mdesc);
	pmwcas_free(mdesc);
	return done;
}

template<typename Key, typename Val>
uint64_t * bz_node<Key, Val>::rec_meta_arr() {
	return (uint64_t*)((char*)this + sizeof(*this));
}
/* K-V getter and setter */
template<typename Key, typename Val>
Key * bz_node<Key, Val>::get_key(uint64_t meta) {
	uint32_t off = get_offset(meta);
	if (is_visiable(meta) && off)
		return (Key*)((char*)this + off);
	return nullptr;
}
template<typename Key, typename Val>
void bz_node<Key, Val>::set_key(uint32_t offset, const Key *key) {
	Key * addr = (Key *)((char *)this + offset);
	if (typeid(Key) == typeid(char))
		strcpy((char *)addr, (char*)key);
	else
		*addr = *key;
}
template<typename Key, typename Val>
Val * bz_node<Key, Val>::get_value(uint64_t meta) {
	uint32_t off = get_offset(meta);
	if (is_visiable(meta) && off)
		return (Val*)((char*)this + off + get_key_length(meta));
	return nullptr;
}
template<typename Key, typename Val>
void bz_node<Key, Val>::set_value(uint32_t offset, const Val * val) {
	Val * addr = (Val *)((char *)this + offset);
	if (typeid(Val) == typeid(char))
		strcpy((char*)addr, (char*)val);
	else
		*addr = *val;
}

/* 键值比较函数 */
template<typename Key, typename Val>
int bz_node<Key, Val>::key_cmp(uint64_t meta_entry, const Key * key) {
	const Key * k1 = get_key(meta_entry);
	if (typeid(Key) == typeid(char))
		return strcmp((char*)k1, (char*)key);
	if (*k1 == *key)
		return 0;
	return *k1 < *key ? -1 : 1;
}