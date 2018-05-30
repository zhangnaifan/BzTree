#ifndef BZTREE_H
#define BZTREE_H

#include <thread>
#include <vector>
#include <tuple>
#include <algorithm>
#include <stack>
#include <queue>

#include "bzerrno.h"
#include "PMwCAS.h"

#include <mutex>
#include <fstream>

#define BZ_SPLIT			1
#define BZ_MERGE			2
#define BZ_CONSOLIDATE		3

const uint64_t BZ_KEY_MAX = 0xdeadbeafdeadbeaf;

#define BZ_TYPE_LEAF		1
#define BZ_TYPE_NON_LEAF	2

template<typename Key, typename Val>
struct bz_tree;

#ifdef BZ_DEBUG
#include <iomanip>
std::mutex mylock;
std::fstream fs("log.txt", std::ios::app);
#endif // BZ_DEBUG

struct bz_node_fake {};
POBJ_LAYOUT_BEGIN(layout_name);
POBJ_LAYOUT_TOID(layout_name, struct bz_node_fake);
POBJ_LAYOUT_END(layout_name);

/* BzTree节点头部 */
//如果是叶节点，则Val代表实际数据的类型，否则Val为uint64_t，代表孩子节点的相对指针
template<typename Key, typename Val>
struct bz_node
{
	/* status 3: PMwCAS control, 1: frozen, 16: record count, 22: block size, 22: delete size */
	uint64_t status_;
	/* length 32: node size, 31: sorted count, 1: is_leaf */
	uint64_t length_;
	/* record meta entry 3: PMwCAS control, 1: visiable, 28: offset, 16: key length, 16: total length */
	uint64_t * rec_meta_arr();
	
	/* K-V getter and setter */
	Key * get_key(uint64_t meta);
	void set_key(uint32_t offset, const Key * key);
	void copy_key(Key * dst, const Key * src);
	Val * get_value(uint64_t meta);
	void set_value(uint32_t offset, const Val * val);
	void copy_value(Val * dst, const Val * src);
	
	/* 键值比较函数 */
	int key_cmp(uint64_t meta_1, const Key * key);
	bool key_cmp_meta(uint64_t meta_1, uint64_t meta_2);
	
	/* 辅助函数 */
	/* 基本操作 */
	uint32_t binary_search(int size, const Key * key);
	bool pack_pmwcas(mdesc_pool_t pool, std::vector<std::tuple<rel_ptr<uint64_t>, uint64_t, uint64_t>> casn);
	bool find_key_sorted(const Key * key, uint32_t &pos);
	bool find_key_unsorted(const Key * key, uint64_t status_rd, uint32_t alloc_epoch, uint32_t &pos, bool &recheck);
	uint64_t status_add_rec_blk(uint64_t status_rd, uint32_t total_size);
	uint64_t status_del(uint64_t status_rd, uint32_t total_size);
	uint64_t status_frozen(uint64_t status_rd);
	uint64_t meta_vis_off(uint64_t meta_rd, bool set_vis, uint32_t new_offset);
	uint64_t meta_vis_off_klen_tlen(uint64_t meta_rd, bool set_vis, uint32_t new_offset, uint32_t key_size, uint32_t total_size);
	void copy_data(uint32_t new_offset, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size);
	void copy_data(uint64_t meta_rd, std::vector<std::pair<std::shared_ptr<Key>, std::shared_ptr<Val>>> & res);
	int rescan_unsorted(uint32_t beg_pos, uint32_t rec_cnt, const Key * key, uint32_t total_size, uint32_t alloc_epoch);

	/* SMO辅助函数 */
	int triger_consolidate();
	uint32_t copy_meta(rel_ptr<bz_node<Key, Val>> dst, uint32_t rec_cnt = 0, bool sort = true, uint32_t offset = 0);
	uint32_t copy_payload(rel_ptr<bz_node<Key, Val>> dst, uint32_t new_rec_cnt, uint32_t offset = 0, uint32_t exist_blk_size = 0);
	void init_header(rel_ptr<bz_node<Key, Val>> dst, uint32_t new_rec_cnt, uint32_t blk_sz, int leaf_opt = 0, uint32_t dele_sz = 0);
	uint32_t get_balanced_count(rel_ptr<bz_node<Key, Val>> dst);
	bool try_freeze();
	void unfreeze();
	rel_ptr<uint64_t> nth_child(int n);
	Key * nth_key(int n);
	Val * nth_val(int n);
	
	template<typename TreeVal>
	int consolidate(bz_tree<Key, TreeVal> * tree, rel_ptr<uint64_t> parent_status, rel_ptr<uint64_t> parent_ptr);
	template<typename TreeVal>
	int split(bz_tree<Key, TreeVal> * tree, rel_ptr<bz_node<Key, uint64_t>> parent, rel_ptr<uint64_t> grandpa_status, rel_ptr<uint64_t> grandpa_ptr);
	template<typename TreeVal>
	int merge(bz_tree<Key, TreeVal> * tree, int child_id, rel_ptr<bz_node<Key, uint64_t>> parent, rel_ptr<uint64_t> grandpa_status, rel_ptr<uint64_t> grandpa_ptr);
	
	/* 执行叶节点的数据项操作 */
	int insert(bz_tree<Key, Val> * tree, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size, uint32_t alloc_epoch);
	int remove(bz_tree<Key, Val> * tree, const Key * key);
	int update(bz_tree<Key, Val> * tree, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size, uint32_t alloc_epoch);
	int   read(bz_tree<Key, Val> * tree, const Key * key, Val * val, uint32_t max_val_size);
	int upsert(bz_tree<Key, Val> * tree, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size, uint32_t alloc_epoch);
	std::vector<std::pair<std::shared_ptr<Key>, std::shared_ptr<Val>>> range_scan(const Key * beg_key, const Key * end_key);

#ifdef BZ_DEBUG
	void print_log(const char * action, const Key * k, int ret = -1, bool pr = true);

#endif // BZ_DEBUG
};

/* BzTree */
template<typename Key, typename Val>
struct bz_tree {
	PMEMobjpool *				pop_;
	pmwcas_pool					pool_;
	uint64_t					root_;
	uint32_t					epoch_;

	void first_use(uint32_t node_size = 1024 * 1024 * 8);
	int init(PMEMobjpool * pop, PMEMoid base_oid);
	void recovery();
	void finish();
	int new_root();
	//int insert(const Key * key, const Val * val, uint32_t key_size, uint32_t total_size);

	/* 辅助函数 */
	int acquire_wr(std::stack<std::pair<uint64_t, int>> path_stack);
	void acquire_rd();
	void release();
	template<typename NType>
	int smo(std::stack<std::pair<uint64_t, int>>& path_stack);

#ifdef BZ_DEBUG
	void print_tree();
	void print_dfs(uint64_t ptr, int level);

#endif // BZ_DEBUG
};

/*
merge N
1. choose N's left/right sibling if
	1.1 shares the same parent
	1.2 small enough to absorb N's records
2. if neither is ok, return false
3. freeze N and L status
4. allocate 2 new nodes:
	4.1 new node N' contains N and L's records
	4.2 N and L parent P' that swaps the child ptr to L to N' 
5. 3-word PMwCAS
	5.1 G's child ptr to P
	5.2 G's status
	5.3 freeze P
*/
template<typename Key, typename Val>
template<typename TreeVal>
int bz_node<Key, Val>::merge(bz_tree<Key, TreeVal> * tree, int child_id,
	rel_ptr<bz_node<Key, uint64_t>> parent,
	rel_ptr<uint64_t> grandpa_status, rel_ptr<uint64_t> grandpa_ptr)
{
	/* Global Variables */
	uint64_t status_parent;
	uint64_t status_cur;
	uint64_t status_sibling;
	int sibling_type = 0; //-1: 左 1: 右
	rel_ptr<bz_node<Key, Val>> sibling;
	rel_ptr<bz_node<Key, uint64_t>> new_parent;
	uint32_t child_max;
	int ret = 0;
	bool forbids[2] = { false, false };

	//根节点无法merge
	if (parent.is_null())
		return ENOTFOUND;

	//寻找兄弟节点
	while (true) {

		status_cur = pmwcas_read(&status_);
		if (is_frozen(status_cur))
			return EFROZEN;
		uint32_t cur_sz = use_size(status_cur);

		status_parent = pmwcas_read(&parent->status_);
		if (is_frozen(status_parent))
			return EFROZEN;
		child_max = get_record_count(status_parent);
		sibling_type = 0;
		if (!forbids[0] && child_id > 0) {
			//判断左兄弟
			sibling = parent->nth_child(child_id - 1);
			status_sibling = pmwcas_read(&sibling->status_);
			if (!is_frozen(status_sibling)) {
				uint32_t left_sz = use_size(status_sibling);
				if (cur_sz + left_sz + sizeof(*this) < NODE_SPLIT_SIZE) {
					sibling_type = -1;
				}
				else {
					forbids[0] = true;
				}
			}
			else {
				forbids[0] = true;
			}
		}
		if (!forbids[1] && !sibling_type && (uint32_t)child_id + 1 < child_max) {
			//判断右兄弟
			sibling = parent->nth_child(child_id + 1);
			status_sibling = pmwcas_read(&sibling->status_);
			if (!is_frozen(status_sibling)) {
				uint32_t right_sz = use_size(status_sibling);
				if (cur_sz + right_sz + sizeof(*this) < NODE_SPLIT_SIZE) {
					sibling_type = 1;
				}
				else {
					forbids[1] = true;
				}
			}
			else {
				forbids[1] = true;
			}
		}
		if (!sibling_type) {
			//没有合适的
			return ENOTFOUND;
		}
		//尝试freeze当前节点和兄弟节点
		uint64_t status_cur_new = status_frozen(status_cur);
		uint64_t status_cur_rd = CAS(&status_, status_cur_new, status_cur);
		if (is_frozen(status_cur_rd))
			return EFROZEN;
		if (status_cur_rd == status_cur) {
			uint64_t status_sibling_new = status_frozen(status_sibling);
			uint64_t status_sibling_rd = CAS(&sibling->status_,
				status_sibling_new, status_sibling);
			if (is_frozen(status_sibling_rd))
				forbids[sibling_type < 0 ? 0 : 1] = true;
			else if (status_sibling_rd == status_sibling)
				break; //成功
		}

		//判断是否有必要merge
		if (triger_consolidate() != BZ_MERGE)
			return ERACE;

		if (forbids[0] && forbids[1])
			return ENOTFOUND;
	}

	//申请真正的mdesc
	mdesc_t mdesc = pmwcas_alloc(&tree->pool_);
	while (mdesc.is_null()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		mdesc = pmwcas_alloc(&tree->pool_);
	}
	//freeze parent
	uint64_t status_parent_new = status_frozen(status_parent);
	pmwcas_add(mdesc, &parent->status_, status_parent, status_parent_new);

	/* 分配N'、P' */
	//申请暂存内存的mdesc
	mdesc_t tmp_mdesc = pmwcas_alloc(&tree->pool_, RELEASE_NEW_ON_FAILED);
	while (tmp_mdesc.is_null()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		tmp_mdesc = pmwcas_alloc(&tree->pool_, RELEASE_NEW_ON_FAILED);
	}
	rel_ptr<rel_ptr<bz_node<Key, Val>>> new_node_ptr =
		pmwcas_reserve<bz_node<Key, Val>>(tmp_mdesc, get_magic(&tree->pool_, 0), this);
	//原子分配空间
	TX_BEGIN(tree->pop_) {
		pmemobj_tx_add_range_direct(new_node_ptr.abs(), sizeof(uint64_t));
		*new_node_ptr = pmemobj_tx_alloc(NODE_ALLOC_SIZE, TOID_TYPE_NUM(struct bz_node_fake));
	} TX_END;
	assert(!new_node_ptr->is_null());

	rel_ptr<bz_node<Key, Val>> new_node = *new_node_ptr;

	/* 初始化N' BEGIN */
	uint32_t new_blk_sz = 0;
	uint32_t new_rec_cnt = 0;
	
	//拷贝N和sibling的meta和k-v
	uint32_t cur_rec_cnt = copy_meta(new_node, 0, false);
	uint32_t cur_blk_sz = copy_payload(new_node, cur_rec_cnt);
	uint32_t tot_rec_cnt = sibling->copy_meta(new_node, 0, false, cur_rec_cnt);
	new_blk_sz = sibling->copy_payload(new_node, tot_rec_cnt - cur_rec_cnt, cur_rec_cnt, cur_blk_sz);
	
	//排序
	uint64_t * new_meta_arr = new_node->rec_meta_arr();
	std::sort(new_meta_arr, new_meta_arr + tot_rec_cnt,
		std::bind(&bz_node<Key, Val>::key_cmp_meta, (bz_node<Key, Val>*)new_node.abs(), std::placeholders::_1, std::placeholders::_2));
	//计算有效meta数目
	new_rec_cnt = new_node->binary_search(tot_rec_cnt, nullptr);
	//初始化status和length
	init_header(new_node, new_rec_cnt, new_blk_sz);
	persist(new_node.abs(), NODE_ALLOC_SIZE);
	/* 初始化N' END */

	if (child_max > 2 || child_max == 2 && BZ_KEY_MAX != *(uint64_t*)parent->nth_key(1)) {
		/* 不需要替换父节点 */
		/* 初始化P' BEGIN */
		rel_ptr<rel_ptr<bz_node<Key, uint64_t>>> new_parent_ptr =
			pmwcas_reserve<bz_node<Key, uint64_t>>(tmp_mdesc, get_magic(&tree->pool_, 1),
				rel_ptr<bz_node<Key, uint64_t>>(0xdeadbeaf));
		//原子分配空间
		TX_BEGIN(tree->pop_) {
			pmemobj_tx_add_range_direct(new_parent_ptr.abs(), sizeof(uint64_t));
			*new_parent_ptr = pmemobj_tx_alloc(NODE_ALLOC_SIZE, TOID_TYPE_NUM(struct bz_node_fake));
		} TX_END;
		assert(!new_parent_ptr->is_null());
		new_parent = *new_parent_ptr;

		//拷贝meta和k-ptr
		parent->copy_meta(new_parent, child_max, false);
		uint32_t new_parent_blk_sz = parent->copy_payload(new_parent, child_max);
		//删去较小node的meta，增加delete_size
		int pos = sibling_type < 0 ? child_id - 1 : child_id;
		uint64_t * parent_meta_arr = new_parent->rec_meta_arr();
		uint32_t dele_sz = get_total_length(parent_meta_arr[pos]);
		memmove(parent_meta_arr + pos, parent_meta_arr + pos + 1, (child_max - 1 - pos) * sizeof(uint64_t));
		//替换指针，指向N'
		*new_parent->get_value(parent_meta_arr[pos]) = new_node.rel();
		//增加delete size
		init_header(new_parent, child_max - 1, new_parent_blk_sz, BZ_TYPE_NON_LEAF, dele_sz);
		persist(new_parent.abs(), NODE_ALLOC_SIZE);
		/* 初始化P' END */
		
		//pmwcas
		if (grandpa_ptr.is_null()) {
			pmwcas_add(mdesc, &tree->root_, parent.rel(), new_parent.rel());
		}
		else {
			pmwcas_add(mdesc, grandpa_ptr, parent.rel(), new_parent.rel());
			uint64_t status_grandpa_rd = pmwcas_read(grandpa_status.abs());
			if (is_frozen(status_grandpa_rd)) {
				ret = EFROZEN;
				goto IMMEDIATE_ABORT;
			}
			pmwcas_add(mdesc, grandpa_status, status_grandpa_rd, status_grandpa_rd);
		}
	}
	else {
		//pmwcas
		if (grandpa_ptr.is_null()) {
			pmwcas_add(mdesc, &tree->root_, parent.rel(), new_node.rel());
		}
		else {
			pmwcas_add(mdesc, grandpa_ptr, parent.rel(), new_node.rel());
			uint64_t status_grandpa_rd = pmwcas_read(grandpa_status.abs());
			if (is_frozen(status_grandpa_rd)) {
				ret = EFROZEN;
				goto IMMEDIATE_ABORT;
			}
			pmwcas_add(mdesc, grandpa_status, status_grandpa_rd, status_grandpa_rd);
		}
	}

	//执行pmwcas
	if (!pmwcas_commit(mdesc)) {
		ret = ERACE;
	} 
	else {
		goto FINISH_HERE;
	}

IMMEDIATE_ABORT:
	pmwcas_word_recycle(new_node);
	if (child_max > 1)
		pmwcas_word_recycle(new_parent);
	unfreeze();
	sibling->unfreeze();
FINISH_HERE:
	pmwcas_abort(tmp_mdesc);
	pmwcas_free(mdesc);
	return ret;
}

/*
split
1. freeze the node (k1, k2]
2. scan all valid keys and find the seperator key K
3. allocate 3 new nodes:
	3.1 new N' (K, k2]
	3.2 N' sibling O (k1, K]
	3.3 N' new parent P' (add new key record K and ptr to O)
4. 3-word PMwCAS
	4.1 freeze P status
	4.2 swap G's ptr to P to P'
	4.3 G's status to detect conflicts
NOTES:
	Of new nodes, N' and O are not taken care of.
	So we need an extra PMwCAS mdesc to record those memories:
	in case failure before memory transmition, pmwcas will help reclaim;
	in case success, abort the pmwcas since it has been safe.
*/
template<typename Key, typename Val>
template<typename TreeVal>
int bz_node<Key, Val>::split(
	bz_tree<Key, TreeVal> * tree, rel_ptr<bz_node<Key, uint64_t>> parent, 
	rel_ptr<uint64_t> grandpa_status, rel_ptr<uint64_t> grandpa_ptr)
{
	/* Global Variables */
	uint64_t status_parent_rd;
	int ret = 0;

	//freeze
	if (!try_freeze())
		return EFROZEN;

	//申请真正的mdesc
	mdesc_t mdesc = pmwcas_alloc(&tree->pool_, 0);
	while (mdesc.is_null()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		mdesc = pmwcas_alloc(&tree->pool_);
	}

	/* 分配N'、O、P */
	//申请暂存内存的mdesc
	mdesc_t tmp_mdesc = pmwcas_alloc(&tree->pool_, RELEASE_NEW_ON_FAILED);
	while (tmp_mdesc.is_null()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		tmp_mdesc = pmwcas_alloc(&tree->pool_, RELEASE_NEW_ON_FAILED);
	}

	rel_ptr<rel_ptr<bz_node<Key, Val>>> new_left_ptr =
		pmwcas_reserve<bz_node<Key, Val>>(tmp_mdesc, get_magic(&tree->pool_, 0), this);
	rel_ptr<rel_ptr<bz_node<Key, Val>>> new_right_ptr =
		pmwcas_reserve<bz_node<Key, Val>>(tmp_mdesc, get_magic(&tree->pool_, 1), this);
	rel_ptr<rel_ptr<bz_node<Key, uint64_t>>> new_parent_ptr =
		pmwcas_reserve<bz_node<Key, uint64_t>>(tmp_mdesc, get_magic(&tree->pool_, 2), 
			rel_ptr<bz_node<Key, uint64_t>>(0xdeadbeaf));
	//原子分配空间
	TX_BEGIN(tree->pop_) {
		pmemobj_tx_add_range_direct(new_left_ptr.abs(), sizeof(uint64_t));
		*new_left_ptr = pmemobj_tx_alloc(NODE_ALLOC_SIZE, TOID_TYPE_NUM(struct bz_node_fake));
		pmemobj_tx_add_range_direct(new_right_ptr.abs(), sizeof(uint64_t));
		*new_right_ptr = pmemobj_tx_alloc(NODE_ALLOC_SIZE, TOID_TYPE_NUM(struct bz_node_fake));
		pmemobj_tx_add_range_direct(new_parent_ptr.abs(), sizeof(uint64_t));
		*new_parent_ptr = pmemobj_tx_alloc(NODE_ALLOC_SIZE, TOID_TYPE_NUM(struct bz_node_fake));
	} TX_END;

	assert(!new_left_ptr->is_null() && !new_right_ptr->is_null() 
		&& !new_parent_ptr->is_null());

	/* 初始化N'和O BEGIN */
	rel_ptr<bz_node<Key, Val>> new_left = *new_left_ptr;
	rel_ptr<bz_node<Key, Val>> new_right = *new_right_ptr;
	rel_ptr<bz_node<Key, uint64_t>> new_parent = *new_parent_ptr;
	//拷贝meta到new_left并按键值排序
	uint32_t new_rec_cnt = copy_meta(new_left);
	//按照大小平均分配键值对
	uint32_t left_rec_cnt = get_balanced_count(new_left);
	uint32_t right_rec_cnt = new_rec_cnt - left_rec_cnt;
	//无需split
	if (right_rec_cnt == 0 || right_rec_cnt == 1 && *(uint64_t*)nth_key(0) == BZ_KEY_MAX) {
		pmwcas_word_recycle(new_left);
		pmwcas_word_recycle(new_right);
		pmwcas_word_recycle(new_parent);
		pmwcas_abort(tmp_mdesc);
		pmwcas_free(mdesc);
		unfreeze();
		return EFROZEN;
	}
	//拷贝meta到new_right
	memcpy(new_right->rec_meta_arr(), new_left->rec_meta_arr() + left_rec_cnt, 
		right_rec_cnt * sizeof(uint64_t));
	//memset(new_left->rec_meta_arr() + left_rec_cnt, 0, right_rec_cnt * sizeof(uint64_t));
	//拷贝k-v payload
	uint32_t left_blk_sz = copy_payload(new_left, left_rec_cnt);
	uint32_t right_blk_sz = copy_payload(new_right, right_rec_cnt);
	//初始化status和length
	init_header(new_left, left_rec_cnt, left_blk_sz);
	init_header(new_right, right_rec_cnt, right_blk_sz);
	//持久化
	persist(new_left.abs(), NODE_ALLOC_SIZE);
	persist(new_right.abs(), NODE_ALLOC_SIZE);
	/* 初始化 N'和O END */

	/* 初始化P' BEGIN */
	//获得分割键值K
	uint64_t meta_key = new_left->rec_meta_arr()[left_rec_cnt - 1];
	const Key * K = new_left->get_key(meta_key);
	uint32_t key_sz = get_key_length(meta_key);
	uint32_t tot_sz = key_sz + sizeof(uint64_t);
	uint64_t V = new_left.rel();
	uint64_t * parent_meta_arr = new_parent->rec_meta_arr();

	if (!parent.is_null()) {
		/* 如果当前节点是非根节点 */
		status_parent_rd = pmwcas_read(&parent->status_);
		if (is_frozen(status_parent_rd)) {
			ret = EFROZEN;
			goto IMMEDIATE_ABORT;
		}
		uint32_t rec_cnt = get_record_count(status_parent_rd);
		//拷贝meta, 计算meta数目
		parent->copy_meta(new_parent, rec_cnt, false);
		//拷贝key-value
		uint32_t blk_sz = parent->copy_payload(new_parent, rec_cnt);
		//插入K
		if ((rec_cnt + 2) * sizeof(uint64_t) + key_sz + blk_sz + sizeof(*this) > NODE_ALLOC_SIZE) {
			ret = EALLOCSIZE;
			goto IMMEDIATE_ABORT;
		}
		uint32_t pos = new_parent->binary_search(rec_cnt, K);
		memmove(parent_meta_arr + pos + 1, parent_meta_arr + pos, sizeof(uint64_t) * (rec_cnt - pos));
		uint32_t key_offset = NODE_ALLOC_SIZE - blk_sz - tot_sz - 1;
		parent_meta_arr[pos] = meta_vis_off_klen_tlen(0, true, key_offset, key_sz, tot_sz);
		new_parent->copy_data(key_offset, K, &V, key_sz, tot_sz);
		//修改原来指向N的指针，现在指向new_right
		*new_parent->get_value(parent_meta_arr[pos + 1]) = new_right.rel();
		//初始化节点头的status和length
		parent->init_header(new_parent, 1 + rec_cnt, blk_sz + tot_sz);
		//持久化
		persist(new_parent.abs(), NODE_ALLOC_SIZE);
	}
	else {
		/* 如果当前节点是根节点 */
		//往P'插入<K, new_left>
		uint32_t left_key_offset = NODE_ALLOC_SIZE - tot_sz - 1;
		parent_meta_arr[0] = meta_vis_off_klen_tlen(0, true, left_key_offset, key_sz, tot_sz);
		new_parent->copy_data(left_key_offset, K, &V, key_sz, tot_sz);

		//往P'插入<BZ_KEY_MAX, new_right>
		uint32_t right_key_offset = left_key_offset - sizeof(uint64_t) * 2;
		parent_meta_arr[1] = meta_vis_off_klen_tlen(0, true, right_key_offset, sizeof(uint64_t), sizeof(uint64_t) * 2);
		*(uint64_t*)new_parent->get_key(parent_meta_arr[1]) = BZ_KEY_MAX;
		*new_parent->get_value(parent_meta_arr[1]) = new_right.rel();

		//初始化status和length
		init_header(new_parent, 2, tot_sz + sizeof(uint64_t) * 2, BZ_TYPE_NON_LEAF);
	}
	/* 初始化P' END */

	/* 3-word pmwcas */
	if (!grandpa_ptr.is_null()) {
		/* 存在祖父节点 */
		//3.1 G's ptr to P -> P'
		pmwcas_add(mdesc, grandpa_ptr, parent.rel(), new_parent.rel());
		//3.2 freeze P's status
		uint64_t status_parent_new = status_frozen(status_parent_rd);
		pmwcas_add(mdesc, &parent->status_, status_parent_rd, status_parent_new);
		//3.3 make sure G's status is not frozen
		uint64_t status_grandpa_rd = pmwcas_read(grandpa_status.abs());
		if (is_frozen(status_grandpa_rd)) {
			ret = EFROZEN;
			goto IMMEDIATE_ABORT;
		}
		pmwcas_add(mdesc, grandpa_status, status_grandpa_rd, status_grandpa_rd);
	}
	else if (!parent.is_null()) {
		/* 父节点是根节点 */
		//3.1 root's ptr to P -> P'
		pmwcas_add(mdesc, &tree->root_, parent.rel(), new_parent.rel());
		//3.2 freeze P's status
		uint64_t status_parent_new = status_frozen(status_parent_rd);
		pmwcas_add(mdesc, &parent->status_, status_parent_rd, status_parent_new);
	}
	else {
		/* 当前节点是根节点 */
		rel_ptr<bz_node<Key, Val>> cur_ptr = this;
		pmwcas_add(mdesc, &tree->root_, cur_ptr.rel(), new_parent.rel());
	}

	//执行pmwcas
	if (!pmwcas_commit(mdesc)) {
		ret = ERACE;
	}
	else {
		goto FINISH_HERE;
	}

IMMEDIATE_ABORT:
	pmwcas_word_recycle(new_left);
	pmwcas_word_recycle(new_right);
	pmwcas_word_recycle(new_parent);
	unfreeze();
FINISH_HERE:
	pmwcas_abort(tmp_mdesc);
	pmwcas_free(mdesc);
	return ret;
}

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
template<typename Key, typename Val>
template<typename TreeVal>
int bz_node<Key, Val>::consolidate(bz_tree<Key, TreeVal> * tree, 
	rel_ptr<uint64_t> parent_status, rel_ptr<uint64_t> parent_ptr)
{
	//freeze
	if (!try_freeze())
		return EFROZEN;

	//申请mdesc
	mdesc_t mdesc = pmwcas_alloc(&tree->pool_, 0);
	while (mdesc.is_null()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		mdesc = pmwcas_alloc(&tree->pool_);
	}
	rel_ptr<rel_ptr<bz_node<Key, Val>>> ptr;
	if (!parent_ptr.is_null()) {
		ptr = pmwcas_reserve<bz_node<Key, Val>>(mdesc,
			parent_ptr, this, RELEASE_SWAP_PTR);
	}
	else {
		ptr = pmwcas_reserve<bz_node<Key, Val>>(mdesc,
			(rel_ptr<bz_node<Key, Val>>*)&tree->root_, this, RELEASE_SWAP_PTR);
	}
	//原子分配空间
	TX_BEGIN(tree->pop_) {
		pmemobj_tx_add_range_direct(ptr.abs(), sizeof(uint64_t));
		*ptr = pmemobj_tx_alloc(NODE_ALLOC_SIZE, TOID_TYPE_NUM(struct bz_node_fake));
	} TX_END;

	//初始化节点内容为0
	rel_ptr<bz_node<Key, Val>> node = *ptr;
	//拷贝meta并按键值排序, 计算有效meta数目
	uint32_t new_rec_cnt = copy_meta(node);
	//拷贝key-value
	uint32_t new_blk_sz = copy_payload(node, new_rec_cnt);
	//初始化节点头的status和length
	init_header(node, new_rec_cnt, new_blk_sz);
	//持久化
	persist(node.abs(), NODE_ALLOC_SIZE);

	//如果需要修改父节点，确保其Frozen != 0
	if (!parent_ptr.is_null()) {
		uint64_t status_rd = pmwcas_read(parent_status.abs());
		if (is_frozen(status_rd)) {
			unfreeze();
			pmwcas_free(mdesc);
			return EFROZEN;
		}
		pmwcas_add(mdesc, parent_status, status_rd, status_rd, 0);
	}

	//执行pmwcas
	bool ret = pmwcas_commit(mdesc);
	pmwcas_free(mdesc);
	if (!ret)
		unfreeze();
	return ret ? 0 : ERACE;
}
/* 计算节点均分时坐节点的数据项个数 */
template<typename Key, typename Val>
uint32_t bz_node<Key, Val>::get_balanced_count(rel_ptr<bz_node<Key, Val>> dst)
{
	uint64_t * left_meta_arr = dst->rec_meta_arr();
	uint64_t status_rd = pmwcas_read(&status_);
	uint32_t old_blk_sz = get_block_size(status_rd) - get_delete_size(status_rd);
	uint32_t left_rec_cnt = 0;
	for (uint32_t acc_sz = 0;
		acc_sz < old_blk_sz / 2;
		acc_sz += get_total_length(left_meta_arr[left_rec_cnt++]));
	return left_rec_cnt;
}
/* 从当前节点拷贝meta到dst节点，并按照键值和可见性排序，返回现有数据数量 */
template<typename Key, typename Val>
uint32_t bz_node<Key, Val>::copy_meta(rel_ptr<bz_node<Key, Val>> dst, uint32_t rec_cnt, bool sort, uint32_t offset)
{
	//拷贝meta并按键值排序
	if (!rec_cnt) {
		uint64_t status_rd = pmwcas_read(&status_);
		rec_cnt = get_record_count(status_rd);
	}
	uint64_t * new_meta_arr = dst->rec_meta_arr();
	uint64_t * old_meta_arr = rec_meta_arr();
	for (uint32_t i = 0; i < rec_cnt; ++i)
		new_meta_arr[i + offset] = pmwcas_read(old_meta_arr + i);
	if (sort) {
		std::sort(new_meta_arr, new_meta_arr + offset + rec_cnt,
			std::bind(&bz_node<Key, Val>::key_cmp_meta, this, std::placeholders::_1, std::placeholders::_2));
		//计算有效meta数目
		uint32_t new_rec_cnt = dst->binary_search(offset + rec_cnt, nullptr);
		//将剩余部分置空
		//memset(new_meta_arr + new_rec_cnt, 0, (rec_cnt - new_rec_cnt) * sizeof(uint64_t));
		return new_rec_cnt;
	}
	return offset + rec_cnt;
}
/* 拷贝key-value 并返回操作后的block大小 */
template<typename Key, typename Val>
uint32_t bz_node<Key, Val>::copy_payload(rel_ptr<bz_node<Key, Val>> dst, uint32_t new_rec_cnt, uint32_t offset, uint32_t exist_blk_size)
{
	uint32_t blk_sz = exist_blk_size;
	uint32_t node_sz = NODE_ALLOC_SIZE;
	uint64_t * new_meta_arr = dst->rec_meta_arr();
	for (uint32_t i = offset; i < offset + new_rec_cnt; ++i) {
		if (!is_visiable(new_meta_arr[i]))
			continue;
		const Key * key = get_key(new_meta_arr[i]);
		const Val * val = get_value(new_meta_arr[i]);
		uint32_t key_sz = get_key_length(new_meta_arr[i]);
		uint32_t tot_sz = get_total_length(new_meta_arr[i]);
		uint32_t new_offset = node_sz - blk_sz - tot_sz - 1;
		new_meta_arr[i] = meta_vis_off_klen_tlen(0, true, new_offset, key_sz, tot_sz);
		dst->copy_data(new_offset, key, val, key_sz, tot_sz);
		blk_sz += tot_sz;
	}
	return blk_sz;
}
/* 根据参数初始化status和length，单线程调用 */
template<typename Key, typename Val>
void bz_node<Key, Val>::init_header(rel_ptr<bz_node<Key, Val>> dst, uint32_t new_rec_cnt, uint32_t blk_sz, int leaf_opt, uint32_t dele_sz)
{
	dst->status_ = 0ULL;
	set_record_count(dst->status_, new_rec_cnt);
	set_block_size(dst->status_, blk_sz);
	set_delete_size(dst->status_, dele_sz);
	if (!leaf_opt) {
		if (is_leaf(length_))
			set_leaf(dst->length_);
		else
			set_non_leaf(dst->length_);
	}
	else if (leaf_opt == BZ_TYPE_LEAF)
		set_leaf(dst->length_);
	else
		set_non_leaf(dst->length_);
	set_node_size(dst->length_, NODE_ALLOC_SIZE);
	set_sorted_count(dst->length_, new_rec_cnt);
}

template<typename Key, typename Val>
bool bz_node<Key, Val>::try_freeze()
{
	uint64_t status_rd, status_new;
	do {
		status_rd = pmwcas_read(&status_);
		status_new = status_frozen(status_rd);
	} while (!is_frozen(status_rd) 
		&& status_rd != CAS(&status_, status_new, status_rd));
	return !is_frozen(status_rd);
}

template<typename Key, typename Val>
inline void bz_node<Key, Val>::unfreeze()
{
	uint64_t status_unfrozen = status_;
	unset_frozen(status_unfrozen);
	CAS(&status_, status_unfrozen, status_);
}

template<typename Key, typename Val>
inline rel_ptr<uint64_t> bz_node<Key, Val>::nth_child(int n)
{
	if (n < 0)
		return rel_ptr<uint64_t>::null();
	return rel_ptr<uint64_t>(*nth_val(n));
}

template<typename Key, typename Val>
inline Key * bz_node<Key, Val>::nth_key(int n)
{
	if (n < 0)
		return nullptr;
	return get_key(pmwcas_read(rec_meta_arr() + n));
}

template<typename Key, typename Val>
inline Val * bz_node<Key, Val>::nth_val(int n)
{
	if (n < 0)
		return nullptr;
	return get_value(pmwcas_read(rec_meta_arr() + n));
}

template<typename Key, typename Val>
int bz_node<Key, Val>::triger_consolidate()
{
	uint64_t status_rd = pmwcas_read(&status_);
	uint32_t rec_cnt = get_record_count(status_rd);
	uint32_t blk_sz = get_block_size(status_rd);
	uint32_t dele_sz = get_delete_size(status_rd);
	uint32_t node_sz = get_node_size(length_);
	uint32_t free_sz = node_sz - blk_sz - sizeof(*this) - rec_cnt * sizeof(uint64_t);
	uint32_t new_node_sz_up = blk_sz - dele_sz + rec_cnt * sizeof(uint64_t) + sizeof(*this);
	uint32_t new_node_sz_low = blk_sz - dele_sz + sizeof(*this);
	if (free_sz < NODE_CONSOLIDATE_SIZE) {
		if (new_node_sz_low > NODE_SPLIT_SIZE)
			return BZ_SPLIT;
		else if (new_node_sz_up < NODE_MERGE_SIZE)
			return BZ_MERGE;
		else
			return BZ_CONSOLIDATE;
	}
	return 0;
}

template<typename Key, typename Val>
template<typename NType>
int bz_tree<Key, Val>::smo(std::stack<std::pair<uint64_t, int>>& path_stack)
{
	rel_ptr<bz_node<Key, NType>> node(path_stack.top().first);
	int child_id = path_stack.top().second;
	int smo_type = node->triger_consolidate();
	if (smo_type == BZ_CONSOLIDATE) {
		if (child_id < 0) {
			node->consolidate<Val>(this, rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
		}
		else {
			path_stack.pop();
			assert(!path_stack.empty());
			rel_ptr<bz_node<Key, uint64_t>> parent(path_stack.top().first);
			node->consolidate<Val>(this, &parent->status_, parent->nth_child(child_id));
		}
	}
	else if (smo_type == BZ_SPLIT) {
		if (child_id < 0) {
			node->split<Val>(this, rel_ptr<bz_node<Key, uint64_t>>::null(),
				rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
		}
		else {
			path_stack.pop();
			assert(!path_stack.empty());
			rel_ptr<bz_node<Key, uint64_t>> parent(path_stack.top().first);
			int grand_id = path_stack.top().second;
			if (grand_id < 0) {
				node->split<Val>(this, parent, rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
			}
			else {
				path_stack.pop();
				assert(!path_stack.empty());
				rel_ptr<bz_node<Key, uint64_t>> grandpa(path_stack.top().first);
				node->split<Val>(this, parent, &grandpa->status_, grandpa->nth_child(grand_id));
			}
		}
	}
	else if (smo_type == BZ_MERGE) {

	}
	return smo_type;
}

/* 检查是否需要调整节点结构 & 进入GC临界区 */
/* @param path_stack <节点相对地址, 父节点到孩子指针的相对地址> */
template<typename Key, typename Val>
int bz_tree<Key, Val>::acquire_wr(std::stack<std::pair<uint64_t, int>> path_stack)
{
	//访问节点
	gc_crit_enter(pool_.gc);

	//检查是否需要结构调整SMO
	assert(!path_stack.empty());
	int smo_type = 0;
	uint64_t ptr = path_stack.top().first;
	if (is_leaf_node(ptr)) {
		smo_type = smo<Val>(path_stack);
	}
	else {
		smo_type = smo<uint64_t>(path_stack);
	}
	if (smo_type) {
		gc_crit_exit(pool_.gc);
		return ESMO;
	}
	return 0;
}
/* 进入GC临界区 */
template<typename Key, typename Val>
inline void bz_tree<Key, Val>::acquire_rd()
{
	//进入节点
	gc_crit_enter(pool_.gc);
}
template<typename Key, typename Val>
inline void bz_tree<Key, Val>::release()
{
	//退出节点
	gc_crit_exit(pool_.gc);
}

#ifdef BZ_DEBUG

template<typename Key, typename Val>
void bz_node<Key, Val>::print_log(const char * action, const Key * k, int ret, bool pr)
{
	if (!pr)
		return;
	mylock.lock();
	if (typeid(Key) == typeid(char)) {
		fs << "[" << this_thread::get_id() << "] ";
		fs << action << " key " << k;
		if (ret >= 0)
			fs << " : " << ret;
		fs << endl;
	}
	else {
		fs << "[" << this_thread::get_id() << "] ";
		fs << action << " key " << *k;
		if (ret >= 0)
			fs << " : " << ret;
		fs << endl;
	}
	mylock.unlock();
}

template<typename Key, typename Val>
void bz_tree<Key, Val>::print_dfs(uint64_t ptr, int level)
{
	if (!ptr)
		return;
	bool isLeaf = is_leaf_node(ptr);
	rel_ptr<bz_node<Key, Val>> node(ptr);
	uint64_t * meta_arr = node->rec_meta_arr();
	uint32_t rec_cnt = get_record_count(node->status_);
	for (int i = 0; i < level; ++i)
		fs << "-";
	fs << std::setfill('0') << std::hex << std::setw(16) << node->status_ << " ";
	fs << std::setfill('0') << std::hex << std::setw(16) << node->length_ << " : ";
	for (uint32_t i = 0; i < rec_cnt; ++i) {
		if (!is_visiable(meta_arr[i]))
			continue;
		if (isLeaf) {
			fs << "(";
			if (typeid(Key) == typeid(char))
				fs << (char*)node->get_key(meta_arr[i]);
			else
				fs << *node->get_key(meta_arr[i]);
			fs << ",";
			if (typeid(Val) == typeid(rel_ptr<char>))
				fs << (char*)(*node->get_value(meta_arr[i])).abs();
			else
				fs << **node->get_value(meta_arr[i]);
			fs << ") ";
		}
		else if (*(uint64_t*)node->get_key(meta_arr[i]) != BZ_KEY_MAX) {
			if (typeid(Key) == typeid(char))
				fs << (char*)node->get_key(meta_arr[i]) << " ";
			else
				fs << *node->get_key(meta_arr[i]) << " ";
		}
		else {
			fs << "KEY_MAX";
		}
	}
	fs << "\n";
	if (!isLeaf) {
		for (uint32_t i = 0; i < rec_cnt; ++i) {
			print_dfs(*(uint64_t*)node->get_value(meta_arr[i]), level + 1);
		}
	}
}

/* 打印树结构，单线程调用 */
template<typename Key, typename Val>
void bz_tree<Key, Val>::print_tree()
{
	mylock.lock();
	print_dfs(root_, 1);
	fs << "\n\n";
	mylock.unlock();
}

#endif // BZ_DEBUG

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
		rec_cnt = get_record_count(status_rd);
		blk_sz = get_block_size(status_rd);

		/* 可容纳 */
		if (blk_sz + total_size + sizeof(uint64_t) * (1 + rec_cnt) + sizeof(*this) > node_sz)
			return EALLOCSIZE;

		/* 增加record count和block size */
		uint64_t status_new = status_add_rec_blk(status_rd, total_size);

		/* 设置offset为alloc_epoch, unset visiable bit */
		uint64_t meta_rd = pmwcas_read(&meta_arr[rec_cnt]);
		meta_new = meta_vis_off(0, false, alloc_epoch);

		/* 2-word PMwCAS */
		if (pack_pmwcas(&tree->pool_, {
			{ &status_, status_rd, status_new },
			{ &meta_arr[rec_cnt], meta_rd, meta_new }
			}))
			break;
		recheck = true;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		status_rd = pmwcas_read(&status_);
		if (is_frozen(status_rd))
			return EFROZEN;
	}

	print_log("IS-pos", key, rec_cnt);

	/* copy key and val */
	uint32_t new_offset = node_sz - blk_sz - total_size - 1;
	copy_data(new_offset, key, val, key_size, total_size);

	if (recheck) {
		int ret = rescan_unsorted(sorted_cnt, rec_cnt, key, total_size, alloc_epoch);
		if (ret)
			return ret;

		print_log("IS-recheck", key, rec_cnt);
	}

	/* set visiable; real offset; key_len and tot_len */
	uint64_t meta_new_plus = meta_vis_off_klen_tlen(0, true, new_offset, key_size, total_size);

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

	print_log("IS-finish", key, rec_cnt);
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

	/* check Frozen */
	uint64_t status_rd = pmwcas_read(&status_);
	uint32_t rec_cnt = get_record_count(status_rd);
	if (is_frozen(status_rd))
		return EFROZEN;

	/* 查找目标键值 */
	bool useless;
	if (!find_key_sorted(key, pos) && !find_key_unsorted(key, status_rd, 0, pos, useless))
		return ENOTFOUND;

	while (true)
	{
		uint64_t meta_rd = pmwcas_read(&meta_arr[pos]);
		if (!is_visiable(meta_rd)) {
			/* 遭遇其他线程的竞争删除 */
			return ENOTFOUND;
		}

		/* 增加delete size */
		uint64_t status_new = status_del(status_rd, get_total_length(meta_rd));

		/* unset visible，offset=0*/
		uint64_t meta_new = meta_vis_off(0, false, 0);

		/* 2-word PMwCAS */
		if (pack_pmwcas(&tree->pool_, {
			{ &status_, status_rd, status_new },
			{ &meta_arr[pos], meta_rd, meta_new }
			}))
			break;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		status_rd = pmwcas_read(&status_);
		if (is_frozen(status_rd))
			return EFROZEN;
	}
	return 0;
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

	print_log("UP-find", key, del_pos);

	while (true)
	{
		rec_cnt = get_record_count(status_rd);
		blk_sz = get_block_size(status_rd);

		/* 可容纳 */
		if (blk_sz + total_size + sizeof(uint64_t) * (1 + rec_cnt) + sizeof(*this) > node_sz)
			return EALLOCSIZE;

		/* 增加record count和block size */
		uint64_t status_new = status_add_rec_blk(status_rd, total_size);

		/* 设置offset为alloc_epoch, unset visiable bit */
		uint64_t meta_rd = pmwcas_read(&meta_arr[rec_cnt]);
		meta_new = meta_vis_off(0, false, alloc_epoch);

		/* 2-word PMwCAS */
		if (pack_pmwcas(&tree->pool_, {
			{ &status_, status_rd, status_new },
			{ &meta_arr[rec_cnt], meta_rd, meta_new }
			}))
			break;
		recheck = true;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		status_rd = pmwcas_read(&status_);
		if (is_frozen(status_rd))
			return EFROZEN;
	}
	
	print_log("UP-pos", key, rec_cnt);

	/* copy key and val */
	uint32_t new_offset = node_sz - blk_sz - total_size - 1;
	copy_data(new_offset, key, val, key_size, total_size);

	if (recheck) {
		int ret = rescan_unsorted(del_pos + 1, rec_cnt, key, total_size, alloc_epoch);
		if (ret)
			return ret;

		print_log("UP-recheck", key, rec_cnt);
	}

	/* set visiable; real offset; key_len and tot_len */
	uint64_t meta_new_plus = meta_vis_off_klen_tlen(0, true, new_offset, key_size, total_size);

	while (true)
	{
		//meta_new = pmwcas_read(&meta_arr[rec_cnt]);
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
			return ERACE;
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

	print_log("UP-finish", key, rec_cnt);
	return 0;
}

/*
read
1) binary search on sorted keys
2) linear scan on unsorted keys
3) return the record found
*/
template<typename Key, typename Val>
int bz_node<Key, Val>::read(bz_tree<Key, Val> * tree, const Key * key, Val * val, uint32_t max_val_size)
{
	/* Global variables */
	uint32_t pos;

	/* 查找目标键值 */
	uint64_t status_rd = pmwcas_read(&status_);
	bool useless;
	if (!find_key_sorted(key, pos) && !find_key_unsorted(key, status_rd, 0, pos, useless))
		return ENOTFOUND;

	uint64_t * meta_arr = rec_meta_arr();
	uint64_t meta_rd = pmwcas_read(&meta_arr[pos]);
	if (get_total_length(meta_rd) - get_key_length(meta_rd) > max_val_size)
		return ENOSPACE;

	copy_value(val, get_value(meta_rd));
	return 0;
}

/* 查找key，存在则后续执行update，否则后续执行insert */
/* Upsert */
template<typename Key, typename Val>
int bz_node<Key, Val>::upsert(bz_tree<Key, Val> * tree, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size, uint32_t alloc_epoch)
{
	/* Global variables */
	uint64_t * meta_arr = rec_meta_arr();
	uint32_t node_sz = get_node_size(length_);
	uint32_t sorted_cnt = get_sorted_count(length_);
	bool recheck = false, found = false;
	uint64_t meta_new;
	uint32_t rec_cnt, blk_sz;

	/* not Frozen */
	uint64_t status_rd = pmwcas_read(&status_);
	if (is_frozen(status_rd))
		return EFROZEN;

	/* 查找目标键值的位置 */
	uint32_t del_pos;
	if (find_key_sorted(key, del_pos)
		|| find_key_unsorted(key, status_rd, alloc_epoch, del_pos, recheck))
		found = true;

	print_log("US-find", key, del_pos);

	while (true)
	{
		rec_cnt = get_record_count(status_rd);
		blk_sz = get_block_size(status_rd);

		/* 可容纳 */
		if (blk_sz + total_size + sizeof(uint64_t) * (1 + rec_cnt) + sizeof(*this) > node_sz)
			return EALLOCSIZE;

		/* 增加record count和block size */
		uint64_t status_new = status_add_rec_blk(status_rd, total_size);

		/* 设置offset为alloc_epoch, unset visiable bit */
		uint64_t meta_rd = pmwcas_read(&meta_arr[rec_cnt]);
		meta_new = meta_vis_off(0, false, alloc_epoch);

		/* 2-word PMwCAS */
		if (pack_pmwcas(&tree->pool_, {
			{ &status_, status_rd, status_new },
			{ &meta_arr[rec_cnt], meta_rd, meta_new }
			}))
			break;
		recheck = true;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		status_rd = pmwcas_read(&status_);
		if (is_frozen(status_rd))
			return EFROZEN;
	}

	print_log("US-pos", key, rec_cnt);

	/* copy key and val */
	uint32_t new_offset = node_sz - blk_sz - total_size - 1;
	copy_data(new_offset, key, val, key_size, total_size);

	if (recheck) {
		uint32_t beg_pos = found ? del_pos + 1 : sorted_cnt;
		int ret = rescan_unsorted(beg_pos, rec_cnt, key, total_size, alloc_epoch);
		if (ret)
			return ret;

		print_log("US-recheck", key, rec_cnt);

	}

	/* set visiable; real offset; key_len and tot_len */
	uint64_t meta_new_plus = meta_vis_off_klen_tlen(0, true, new_offset, key_size, total_size);

	while (true)
	{
		//meta_new = pmwcas_read(&meta_arr[rec_cnt]);
		status_rd = pmwcas_read(&status_);
		if (is_frozen(status_rd)) {
			/* frozen set */
			set_offset(meta_arr[rec_cnt], 0);
			persist(&meta_arr[rec_cnt], sizeof(uint64_t));
			return EFROZEN;
		}

		if (found) {
			/* 执行更新操作 */
			uint64_t meta_del = pmwcas_read(&meta_arr[del_pos]);
			if (!is_visiable(meta_del)) {

				print_log("US-RACE", key, del_pos);

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
				return ERACE;
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
		}
		else
		{
			/* 执行插入操作 */
			/* 2-word PMwCAS */
			if (pack_pmwcas(&tree->pool_, {
				{ &status_, status_rd, status_rd },
				{ &meta_arr[rec_cnt], meta_new, meta_new_plus }
				}))
				break;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}

	print_log("US-finish", key, rec_cnt);

	return 0;
}

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

template<typename Key, typename Val>
std::vector<std::pair<std::shared_ptr<Key>, std::shared_ptr<Val>>>
bz_node<Key, Val>::range_scan(const Key * beg_key, const Key * end_key)
{
	std::vector<std::pair<std::shared_ptr<Key>, std::shared_ptr<Val>>> res;
	uint64_t * meta_arr = rec_meta_arr();
	uint32_t sorted_cnt = get_sorted_count(length_);

	uint32_t bin_beg_pos = binary_search(sorted_cnt, beg_key);
	uint32_t bin_end_pos = end_key ? binary_search(sorted_cnt, end_key) : sorted_cnt;
	for (uint32_t i = bin_beg_pos; i < bin_end_pos; ++i) {
		uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
		if (is_visiable(meta_rd) && key_cmp(meta_rd, beg_key) >= 0
			&& (!end_key || key_cmp(meta_rd, end_key) < 0))
			copy_data(meta_rd, res);
	}

	uint64_t status_rd = pmwcas_read(&status_);
	uint32_t rec_cnt = get_record_count(status_rd);
	for (uint32_t i = sorted_cnt; i < rec_cnt; ++i) {
		uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
		if (is_visiable(meta_rd) && key_cmp(meta_rd, beg_key) >= 0
			&& (!end_key || key_cmp(meta_rd, end_key) < 0)) {
			copy_data(meta_rd, res);
		}
	}

	return res;
}

/* 首次使用BzTree */
template<typename Key, typename Val>
void bz_tree<Key, Val>::first_use(uint32_t node_size)
{
	pmwcas_first_use(&pool_);
	root_ = 0;
	persist(&root_, sizeof(uint64_t));
	epoch_ = 1;
	persist(&epoch_, sizeof(uint32_t));
}

/* 初始化BzTree */
template<typename Key, typename Val>
int bz_tree<Key, Val>::init(PMEMobjpool * pop, PMEMoid base_oid)
{
	rel_ptr<bz_node<Key, Val>>::set_base(base_oid);
	rel_ptr<rel_ptr<bz_node<Key, Val>>>::set_base(base_oid);
	rel_ptr<bz_node<Key, uint64_t>>::set_base(base_oid);
	rel_ptr<rel_ptr<bz_node<Key, uint64_t>>>::set_base(base_oid);
	rel_ptr<bz_node<uint64_t, uint64_t>>::set_base(base_oid);
	rel_ptr<Key>::set_base(base_oid);
	rel_ptr<Val>::set_base(base_oid);
	pop_ = pop;
	int ret = pmwcas_init(&pool_, base_oid);
	if (ret)
		return ret;
	gc_register(pool_.gc);
	return 0;
}
/* 恢复BzTree */
template<typename Key, typename Val>
void bz_tree<Key, Val>::recovery()
{
	pmwcas_recovery(&pool_);
	++epoch_;
	persist(&epoch_, sizeof(epoch_));
}
/* 收工 */
template<typename Key, typename Val>
void bz_tree<Key, Val>::finish()
{
	pmwcas_finish(&pool_);
}

template<typename Key, typename Val>
int bz_tree<Key, Val>::new_root() {
	mdesc_t mdesc = pmwcas_alloc(&pool_);
	while (mdesc.is_null()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		mdesc = pmwcas_alloc(&pool_);
	}
	
	rel_ptr<rel_ptr<bz_node<Key, Val>>> ptr = pmwcas_reserve<bz_node<Key, Val>>(
		mdesc, (rel_ptr<bz_node<Key, Val>>*)&root_, rel_ptr<bz_node<Key, Val>>::null(), 
		RELEASE_NEW_ON_FAILED);
	
	TX_BEGIN(pop_) {
		pmemobj_tx_add_range_direct(ptr.abs(), sizeof(uint64_t));
		*ptr = pmemobj_tx_alloc(NODE_ALLOC_SIZE, TOID_TYPE_NUM(struct bz_node_fake));
	} TX_END;
	
	//初始化根节点
	rel_ptr<bz_node<Key, Val>> node = *ptr;
	memset(node.abs(), 0, NODE_ALLOC_SIZE);
	set_node_size(node->length_, NODE_ALLOC_SIZE);
	persist(node.abs(), NODE_ALLOC_SIZE);
	
	bool ret = pmwcas_commit(mdesc);
	pmwcas_free(mdesc);
	return ret ? 0 : ERACE;
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
	pos = binary_search(sorted_cnt, key);
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
	bool ret = false;
	for (uint32_t i = sorted_cnt; i < rec_cnt; ++i)
	{
		uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
		if (is_visiable(meta_rd)) {
			if (!key_cmp(meta_rd, key)) {
				pos = i;
				ret = true;
			}
		}
		else if (!retry && get_offset(meta_rd) == alloc_epoch)
			retry = true;
	}
	return ret;
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
template<typename Key, typename Val>
inline uint64_t bz_node<Key, Val>::status_frozen(uint64_t status_rd)
{
	set_frozen(status_rd);
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
/* 从索引结构拷贝数据到用户空间 */
template<typename Key, typename Val>
void bz_node<Key, Val>::copy_data(uint64_t meta_rd, std::vector<std::pair<std::shared_ptr<Key>, std::shared_ptr<Val>>> & res)
{
	shared_ptr<Key> sp_key;
	if (typeid(Key) == typeid(char)) {
		sp_key = shared_ptr<Key>(new Key[get_key_length(meta_rd)], [](Key *p) {delete[] p; });
		copy_key(sp_key.get(), get_key(meta_rd));
	}
	else {
		sp_key = make_shared<Key>(*get_key(meta_rd));
	}
	shared_ptr<Val> sp_val;
	if (typeid(Val) == typeid(char)) {
		uint32_t val_sz = get_total_length(meta_rd) - get_key_length(meta_rd);
		sp_val = shared_ptr<Val>(new Val[val_sz], [](Val *p) {delete[] p; });
		copy_value(sp_val.get(), get_value(meta_rd));
	}
	else {
		sp_val = make_shared<Val>(*get_value(meta_rd));
	}
	res.emplace_back(make_pair(sp_key, sp_val));
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
* 如果key为空，则代表此函数用于查找排序后的meta数组中的非空meta的个数
*/
template<typename Key, typename Val>
uint32_t bz_node<Key, Val>::binary_search(int size, const Key * key)
{
	uint64_t * meta_arr = rec_meta_arr();
	int left = -1, right = size;
	while (left + 1 < right) {
		int mid = left + (right - left) / 2;
		uint64_t meta_rd = pmwcas_read(&meta_arr[mid]);
		/* 空洞处理 BEGIN */
		if (!is_visiable(meta_rd)) {
			if (!key) {
				//用于查找非空meta
				right = mid;
				continue;
			}
			//用于查找键值位置
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
		
		if (!key) {
			//用于查找非空meta
			left = mid;
			continue;
		}
		//用于查找键值位置
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
	if (!is_visiable(meta) || !off)
		return nullptr;
	return (Key*)((char*)this + off);
}
template<typename Key, typename Val>
void bz_node<Key, Val>::set_key(uint32_t offset, const Key *key) {
	Key * addr = (Key *)((char *)this + offset);
	copy_key(addr, key);
}
template<typename Key, typename Val>
void bz_node<Key, Val>::copy_key(Key * dst, const Key * src) {
	if (*(uint64_t*)src == BZ_KEY_MAX)
		*(uint64_t*)dst = BZ_KEY_MAX;
	else if (typeid(Key) == typeid(char))
		strcpy_s((char *)dst, strlen((char*)src) + 1, (char*)src);
	else
		*dst = *src;
}
template<typename Key, typename Val>
Val * bz_node<Key, Val>::get_value(uint64_t meta) {
	return (Val*)((char*)this + get_offset(meta) + get_key_length(meta));
}
template<typename Key, typename Val>
void bz_node<Key, Val>::set_value(uint32_t offset, const Val * val) {
	Val * addr = (Val *)((char *)this + offset);
	copy_value(addr, val);
}
template<typename Key, typename Val>
void bz_node<Key, Val>::copy_value(Val * dst, const Val * src)
{
	if (typeid(Val) == typeid(char))
		strcpy((char*)dst, (const char*)src);
	else
		*dst = *src;
}
/* 键值比较函数 */
template<typename Key, typename Val>
int bz_node<Key, Val>::key_cmp(uint64_t meta_entry, const Key * key) {
	const Key * k1 = get_key(meta_entry);
	if (*(uint64_t*)k1 == BZ_KEY_MAX)
		return 1;
	if (*(uint64_t*)key == BZ_KEY_MAX)
		return -1;
	if (typeid(Key) == typeid(char))
		return strcmp((char*)k1, (char*)key);
	if (*k1 == *key)
		return 0;
	return *k1 < *key ? -1 : 1;
}
template<typename Key, typename Val>
bool bz_node<Key, Val>::key_cmp_meta(uint64_t meta_1, uint64_t meta_2)
{
	if (!is_visiable(meta_1))
		return false;
	if (!is_visiable(meta_2))
		return true;
	const Key * k2 = get_key(meta_2);
	return key_cmp(meta_1, k2) < 0;
}

/* 位操作函数集 BEGIN */
inline bool is_frozen(uint64_t status) {
	return 1 & (status >> 60);
}
inline void set_frozen(uint64_t &s) {
	s |= (1ULL << 60);
}
inline void unset_frozen(uint64_t &s) {
	s &= ~(1ULL << 60);
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
inline bool is_leaf(uint64_t length) {
	return !(1 & length);
}
inline void set_non_leaf(uint64_t &length) {
	length |= 1;
}
inline void set_leaf(uint64_t &length) {
	length &= (~1ULL);
}
inline uint32_t get_node_size(uint64_t length) {
	return length >> 32;
}
inline void set_node_size(uint64_t &s, uint64_t new_node_size) {
	s = (s & 0xffffffff) | (new_node_size << 32);
}
inline uint32_t get_sorted_count(uint64_t length) {
	return (length >> 1) & 0x7fffffff;
}
inline void set_sorted_count(uint64_t &s, uint32_t new_sorted_count) {
	s = (s & 0xffffffff00000001) | (new_sorted_count << 1);
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

inline bool is_leaf_node(uint64_t node)
{
	rel_ptr<bz_node<uint64_t, uint64_t>> ptr(node);
	return is_leaf(ptr->length_);
}

inline uint32_t use_size(uint64_t status_rd)
{
	uint32_t rec_cnt = get_record_count(status_rd);
	uint32_t blk_sz = get_block_size(status_rd);
	uint32_t dele_sz = get_delete_size(status_rd);
	uint32_t use_sz = blk_sz - dele_sz + rec_cnt * sizeof(uint64_t);
	return use_sz;
}

#endif // !BZTREE_H
