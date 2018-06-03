#ifndef BZTREE_H
#define BZTREE_H

#include <thread>
#include <vector>
#include <tuple>
#include <algorithm>
#include <queue>

#include "bzerrno.h"
#include "PMwCAS.h"
#include "utils.h"

#include <mutex>
#include <fstream>

//Consolidation types
#define BZ_SPLIT			1
#define BZ_MERGE			2
#define BZ_CONSOLIDATE		3
#define BZ_FROZEN			4

//Action types
#define BZ_ACTION_INSERT	1
#define BZ_ACTION_DELETE	2
#define BZ_ACTION_UPDATE	3
#define BZ_ACTION_UPSERT	4
#define BZ_ACTION_READ		5

//Node types
const uint64_t BZ_KEY_MAX = 0xdeadbeafdeadbeaf;
#define BZ_TYPE_LEAF		1
#define BZ_TYPE_NON_LEAF	2

template<typename Key, typename Val>
struct bz_tree;

#ifdef BZ_DEBUG
//Print
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
	uint32_t binary_search(const Key * key, int size = 0, uint64_t * meta_arr = nullptr);
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
	
	uint32_t copy_node_to(rel_ptr<bz_node<Key, Val>> dst, uint64_t status_rd = 0);
	void fr_sort_meta();
	void fr_remove_meta(int pos);
	int fr_insert_meta(const Key * key, uint64_t left, uint32_t key_sz, uint64_t right);
	int fr_root_init(const Key * key, uint64_t left, uint32_t key_sz, uint64_t right);

	uint32_t copy_sort_meta_to(rel_ptr<bz_node<Key, Val>> dst);
	uint32_t copy_payload_to(rel_ptr<bz_node<Key, Val>> dst, uint32_t new_rec_cnt);
	void init_header(rel_ptr<bz_node<Key, Val>> dst, uint32_t new_rec_cnt, uint32_t blk_sz, int leaf_opt = 0, uint32_t dele_sz = 0);
	uint32_t valid_block_size(uint64_t status_rd = 0);
	uint32_t valid_node_size(uint64_t status_rd = 0);
	uint32_t valid_record_count(uint64_t status_rd = 0);
	uint32_t fr_get_balanced_count(rel_ptr<bz_node<Key, Val>> dst);
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
	template<typename TreeVal>
	int insert(bz_tree<Key, TreeVal> * tree, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size, uint32_t alloc_epoch);
	template<typename TreeVal>
	int remove(bz_tree<Key, TreeVal> * tree, const Key * key);
	template<typename TreeVal>
	int update(bz_tree<Key, TreeVal> * tree, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size, uint32_t alloc_epoch);
	template<typename TreeVal>
	int   read(bz_tree<Key, TreeVal> * tree, const Key * key, Val * buffer, uint32_t max_val_size);
	template<typename TreeVal>
	int upsert(bz_tree<Key, TreeVal> * tree, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size, uint32_t alloc_epoch);
	std::vector<std::pair<std::shared_ptr<Key>, std::shared_ptr<Val>>> range_scan(const Key * beg_key, const Key * end_key);

#ifdef BZ_DEBUG
	void print_log(const char * action, const Key * k = nullptr, uint64_t ret = -1, bool pr = true);

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
	int insert(const Key * key, const Val * val, uint32_t key_size, uint32_t total_size);
	int remove(const Key * key);
	int update(const Key * key, const Val * val, uint32_t key_size, uint32_t total_size);
	int upsert(const Key * key, const Val * val, uint32_t key_size, uint32_t total_size);
	int   read(const Key * key, Val * buffer, uint32_t max_val_size);


	/* 辅助函数 */
	void register_this();
	bool acquire_wr(bz_path_stack * path_stack);
	void acquire_rd();
	void release();
	template<typename NType>
	bool smo(bz_path_stack * path_stack);
	int new_root();
	int traverse(int action, bool wr, const Key * key, const Val * val = nullptr, uint32_t key_size = 0, uint32_t total_size = 0, Val * buffer = nullptr, uint32_t max_val_size = 0);

	template<typename NType>
	rel_ptr<bz_node<Key, NType>> alloc_node(mdesc_t mdesc, int magic = 0);
	mdesc_t alloc_mdesc(int recycle = 0);
	bool pack_pmwcas(std::vector<std::tuple<rel_ptr<uint64_t>, uint64_t, uint64_t>> casn);

#ifdef BZ_DEBUG
	void print_node(uint64_t ptr, int extra);
	void print_tree(bool pr = true);
	void print_dfs(uint64_t ptr, int level);

#endif // BZ_DEBUG
};

template<typename Key, typename Val>
int bz_tree<Key, Val>::traverse(int action, bool wr, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size, Val * buffer, uint32_t max_val_size)
{
	register_this();
	if (!pmwcas_read(&root_)) {
		new_root();
	}
	bz_path_stack path_stack;
	while (true)
	{
		path_stack.reset();
		uint64_t root = pmwcas_read(&root_);
		path_stack.push(root, -1);
		while (true)
		{
			if (wr && acquire_wr(&path_stack)) {
				break;
			}
			else if (!wr) {
				acquire_rd();
			}

			uint64_t ptr = path_stack.get_node();
			if (is_leaf_node(ptr)) {
				rel_ptr<bz_node<Key, Val>> node(ptr);
				int ret;
				if (action == BZ_ACTION_INSERT)
					ret = node->insert(this, key, val, key_size, total_size, epoch_);
				else if (action == BZ_ACTION_DELETE)
					ret = node->remove(this, key);
				else if (action == BZ_ACTION_UPDATE)
					ret = node->update(this, key, val, key_size, total_size, epoch_);
				else if (action == BZ_ACTION_UPSERT)
					ret = node->upsert(this, key, val, key_size, total_size, epoch_);
				else
					ret = node->read(this, key, buffer, max_val_size);
				release();
				return ret;
			}
			else {
				rel_ptr<bz_node<Key, uint64_t>> node(ptr);
				int child_id = (int)node->binary_search(key);
				uint64_t next = *node->nth_val(child_id);
				while ((next & MwCAS_BIT || next & DIRTY_BIT || next & RDCSS_BIT)) {
					node->print_log("BUG", nullptr, next);
					next = *node->nth_val(child_id);
					std::this_thread::sleep_for(std::chrono::milliseconds(1));
					//assert(0);
				};
				path_stack.push(next, child_id);
				release();
			}
		}
	}
}

template<typename Key, typename Val>
inline void bz_tree<Key, Val>::register_this()
{
	gc_register(pool_.gc);
}

/* 检查是否需要调整节点结构 & 进入GC临界区 */
/* @param path_stack <节点相对地址, 父节点到孩子指针的相对地址> */
template<typename Key, typename Val>
bool bz_tree<Key, Val>::acquire_wr(bz_path_stack * path_stack)
{
	//访问节点
	gc_crit_enter(pool_.gc);

	//检查是否需要结构调整SMO
	bool smo_type = false;
	uint64_t ptr = path_stack->get_node();
	if (is_leaf_node(ptr)) {
		smo_type = smo<Val>(path_stack);
	}
	else {
		smo_type = smo<uint64_t>(path_stack);
	}
	if (smo_type) {
		gc_crit_exit(pool_.gc);
	}
	return smo_type;
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

template<typename Key, typename Val>
template<typename NType>
bool bz_tree<Key, Val>::smo(bz_path_stack * path_stack)
{
	rel_ptr<bz_node<Key, NType>> node(path_stack->get_node());
	int child_id = path_stack->get_child_id();
	int smo_type = node->triger_consolidate();
	int ret = 0;
	if (smo_type == BZ_CONSOLIDATE) {
		if (child_id < 0) {
			ret = node->consolidate<Val>(this, rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
		}
		else {
			path_stack->pop();
			rel_ptr<bz_node<Key, uint64_t>> parent(path_stack->get_node());
			ret = node->consolidate<Val>(this, &parent->status_, parent->nth_val(child_id));
			path_stack->push();
		}
	}
	else if (smo_type == BZ_SPLIT) {
		if (child_id < 0) {
			ret = node->split<Val>(this, rel_ptr<bz_node<Key, uint64_t>>::null(),
				rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
		}
		else {
			path_stack->pop();
			rel_ptr<bz_node<Key, uint64_t>> parent(path_stack->get_node());
			int grand_id = path_stack->get_child_id();
			if (grand_id < 0) {
				ret = node->split<Val>(this, parent, rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
			}
			else {
				path_stack->pop();
				rel_ptr<bz_node<Key, uint64_t>> grandpa(path_stack->get_node());
				ret = node->split<Val>(this, parent, &grandpa->status_, grandpa->nth_val(grand_id));
				path_stack->push();
			}
			path_stack->push();
		}
	}
	else if (smo_type == BZ_MERGE) {
		if (child_id < 0)
			return 0;
		path_stack->pop();
		rel_ptr<bz_node<Key, uint64_t>> parent(path_stack->get_node());
		int grand_id = path_stack->get_child_id();
		if (grand_id < 0) {
			ret = node->merge<Val>(this, child_id, parent, rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
		}
		else {
			path_stack->pop();
			rel_ptr<bz_node<Key, uint64_t>> grandpa(path_stack->get_node());
			ret = node->merge<Val>(this, child_id, parent, &grandpa->status_, grandpa->nth_val(grand_id));
			path_stack->push();
		}
		path_stack->push();
	}
	if (ret == EFROZEN) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return smo_type && (ret == EFROZEN || !ret);
}

template<typename Key, typename Val>
template<typename NType>
rel_ptr<bz_node<Key, NType>> bz_tree<Key, Val>::alloc_node(mdesc_t mdesc, int magic)
{
	rel_ptr<rel_ptr<bz_node<Key, NType>>> new_node_ptr =
		pmwcas_reserve<bz_node<Key, NType>>(mdesc, get_magic(&pool_, magic), 
			rel_ptr<bz_node<Key, NType>>(0xabcddeadbeaf));
	//原子分配空间
	TX_BEGIN(pop_) {
		pmemobj_tx_add_range_direct(new_node_ptr.abs(), sizeof(uint64_t));
		*new_node_ptr = pmemobj_tx_alloc(NODE_ALLOC_SIZE, TOID_TYPE_NUM(struct bz_node_fake));
	} TX_END;
	assert(!new_node_ptr->is_null());

	rel_ptr<bz_node<Key, Val>> new_node = *new_node_ptr;
	memset(new_node.abs(), 0, NODE_ALLOC_SIZE);
	set_node_size(new_node->length_, NODE_ALLOC_SIZE);
	return new_node;
}

template<typename Key, typename Val>
int bz_node<Key, Val>::triger_consolidate()
{
	uint64_t status_rd = pmwcas_read(&status_);
	if (is_frozen(status_rd))
		return BZ_FROZEN;
	uint32_t rec_cnt = get_record_count(status_rd);
	uint32_t blk_sz = get_block_size(status_rd);
	uint32_t dele_sz = get_delete_size(status_rd);
	uint32_t node_sz = get_node_size(length_);
	uint32_t free_sz = node_sz - blk_sz - sizeof(*this) - rec_cnt * sizeof(uint64_t);
	uint32_t new_node_sz = valid_node_size(status_rd);
	if (free_sz <= NODE_MIN_FREE_SIZE || dele_sz >= NODE_MAX_DELETE_SIZE) {
		if (new_node_sz >= NODE_SPLIT_SIZE)
			return BZ_SPLIT;
		else if (new_node_sz <= NODE_MERGE_SIZE)
			return BZ_MERGE;
		else
			return BZ_CONSOLIDATE;
	}
	return 0;
}

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
	rel_ptr<bz_node<Key, Val>> new_node;
	uint32_t child_max;
	int ret = 0;
	bool forbids[2] = { false, false };

	//寻找兄弟节点
	while (true) {

		//读当前节点
		status_cur = pmwcas_read(&status_);
		if (is_frozen(status_cur))
			return EFROZEN;

		//如果没有数据，删除节点
		uint32_t valid_rec_cnt = valid_record_count(status_cur);
		if (!valid_rec_cnt) {
			//try to freeze this node
			uint64_t status_cur_new = status_frozen(status_cur);
			if (status_cur == CAS(&status_, status_cur_new, status_cur)) {
				if (!parent.is_null()) {
					//读父节点
					status_parent = pmwcas_read(&parent->status_);
					if (is_frozen(status_parent))
						return EFROZEN;
					child_max = get_record_count(status_parent);
				}
				break;
			}
		}

		//根节点无法merge
		if (parent.is_null())
			return ENONEED;

		//读父节点
		status_parent = pmwcas_read(&parent->status_);
		if (is_frozen(status_parent))
			return EFROZEN;
		child_max = get_record_count(status_parent);
		
		uint32_t cur_sz = valid_node_size(status_cur);

		//选择兄弟节点
		sibling_type = 0;
		if (!forbids[0] && child_id > 0) {
			//判断左兄弟
			sibling = parent->nth_child(child_id - 1);
			status_sibling = pmwcas_read(&sibling->status_);
			if (!is_frozen(status_sibling)) {
				uint32_t left_sz = sibling->valid_node_size(status_sibling);
				if (cur_sz + left_sz - sizeof(*this) < NODE_SPLIT_SIZE) {
					sibling_type = -1;
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
				uint32_t right_sz = sibling->valid_node_size(status_sibling);
				if (cur_sz + right_sz - sizeof(*this) < NODE_SPLIT_SIZE) {
					sibling_type = 1;
				}
			}
			else {
				forbids[1] = true;
			}
		}
		if (!sibling_type) {
			//没有合适的
			return ENONEED;
		}
		
		//尝试freeze当前节点和兄弟节点
		uint64_t status_cur_new = status_frozen(status_cur);
		uint64_t status_sibling_new = status_frozen(status_sibling);
		if (tree->pack_pmwcas({
			{ &status_, status_cur, status_cur_new },
			{ &sibling->status_, status_sibling, status_sibling_new }
			}))
			break;

		//判断是否有必要merge
		if (triger_consolidate() != BZ_MERGE 
			|| (forbids[0] && forbids[1]))
			return ENONEED;
	}

	print_log("MERGE_BEGIN", NULL, sibling.rel());

	//删除根节点
	if (parent.is_null()) {
		CAS(&tree->root_, 0, rel_ptr<bz_node<Key, Val>>(this).rel());
		return 0;
	}

	//申请真正的mdesc
	mdesc_t mdesc = tree->alloc_mdesc();
	//freeze old parent
	uint64_t status_parent_new = status_frozen(status_parent);
	pmwcas_add(mdesc, &parent->status_, status_parent, status_parent_new);

	//申请暂存内存的mdesc
	mdesc_t tmp_mdesc = tree->alloc_mdesc(RELEASE_NEW_ON_FAILED);

	if (sibling_type) {
		/* 初始化N' */
		new_node = tree->alloc_node<Val>(tmp_mdesc, 0);

		uint32_t new_blk_sz = 0;
		uint32_t new_rec_cnt = 0;

		//拷贝N和sibling的meta和k-v
		this->copy_node_to(new_node);
		uint32_t tot_rec_cnt = sibling->copy_node_to(new_node);
		new_node->fr_sort_meta();

		//初始化status和length
		persist(new_node.abs(), NODE_ALLOC_SIZE);
	}

	if (child_max > 2 
		|| child_max == 2 && BZ_KEY_MAX != *(uint64_t*)parent->nth_key(1)
		|| new_node.is_null())
	{
		//保留父亲节点
		
		/* 初始化P' BEGIN */
		new_parent = tree->alloc_node<uint64_t>(tmp_mdesc, 1);

		uint32_t new_parent_rec_cnt = parent->copy_node_to(new_parent) - 1;
		int pos = sibling_type < 0 ? child_id - 1 : child_id;
		
		new_parent->fr_remove_meta(pos);
		if (sibling_type)
			*new_parent->nth_val(pos) = new_node.rel();
		set_sorted_count(new_parent->length_, new_parent_rec_cnt);
		persist(new_parent.abs(), NODE_ALLOC_SIZE);

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
		//删除父亲节点

		if (grandpa_ptr.is_null()) {
			pmwcas_add(mdesc, &tree->root_, parent.rel(), new_node.rel());
		}
		//else if (!new_node.is_null()) {
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
	if (!new_node.is_null()) {
		pmwcas_word_recycle(new_node);
		sibling->unfreeze();
	}
	if (!new_parent.is_null())
		pmwcas_word_recycle(new_parent);
	unfreeze();
FINISH_HERE:
	pmwcas_abort(tmp_mdesc);
	pmwcas_free(mdesc);

	print_log("MERGE_END", NULL, ret);
	if (!ret) {
		if (!new_node.is_null())
			new_node->print_log("MERGE_NEW");
		if (!new_parent.is_null())
			new_parent->print_log("MERGE_NEW_PARENT");
	}

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

	print_log("SPLIT_BEGIN");

	//申请真正的mdesc
	mdesc_t mdesc = tree->alloc_mdesc();

	/* 分配N'、O、P */
	//申请暂存内存的mdesc
	mdesc_t tmp_mdesc = tree->alloc_mdesc(RELEASE_NEW_ON_FAILED);

	rel_ptr<bz_node<Key, Val>> new_left = tree->alloc_node<Val>(tmp_mdesc, 0);
	rel_ptr<bz_node<Key, Val>> new_right = tree->alloc_node<Val>(tmp_mdesc, 1);
	rel_ptr<bz_node<Key, uint64_t>> new_parent = tree->alloc_node<uint64_t>(tmp_mdesc, 2);
	
	/* 初始化N'和O BEGIN */
	//拷贝meta到new_left并按键值排序
	uint32_t new_rec_cnt = this->copy_sort_meta_to(new_left);
	if (new_rec_cnt < 2) {
		pmwcas_word_recycle(new_left);
		pmwcas_word_recycle(new_right);
		pmwcas_word_recycle(new_parent);
		pmwcas_abort(tmp_mdesc);
		pmwcas_free(mdesc);
		unfreeze();
		
		print_log("SPLIT_FAIL");

		return ENONEED;
	}
	//按照大小平均分配键值对
	uint32_t left_rec_cnt = this->fr_get_balanced_count(new_left);
	//保证至少有一个到右节点
	if (left_rec_cnt == new_rec_cnt) {
		--left_rec_cnt;
	}
	uint32_t right_rec_cnt = new_rec_cnt - left_rec_cnt;
	//拷贝meta到new_right
	memcpy(new_right->rec_meta_arr(), new_left->rec_meta_arr() + left_rec_cnt,
		right_rec_cnt * sizeof(uint64_t));
	//memset(new_left->rec_meta_arr() + left_rec_cnt, 0, right_rec_cnt * sizeof(uint64_t));
	//拷贝k-v payload
	uint32_t left_blk_sz = this->copy_payload_to(new_left, left_rec_cnt);
	uint32_t right_blk_sz = this->copy_payload_to(new_right, right_rec_cnt);
	//初始化status和length
	this->init_header(new_left, left_rec_cnt, left_blk_sz);
	this->init_header(new_right, right_rec_cnt, right_blk_sz);
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

		uint32_t new_parent_rec_cnt = parent->copy_node_to(new_parent, status_parent_rd);
		if (ret = new_parent->fr_insert_meta(K, V, key_sz, new_right.rel()))
			goto IMMEDIATE_ABORT;
		
		//持久化
		persist(new_parent.abs(), NODE_ALLOC_SIZE);
	}
	else {
		
		/* 如果当前节点是根节点 */
		
		if (ret = new_parent->fr_root_init(K, V, key_sz, new_right.rel()))
			goto IMMEDIATE_ABORT;
		persist(new_parent.abs(), NODE_ALLOC_SIZE);
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

	print_log("SPLIT_END", NULL, ret);
	if (!ret) {
		string msg = "SPLIT";
		if (!grandpa_ptr.is_null())
			msg += "_3_";
		else if (!parent.is_null())
			msg += "_2_";
		else
			msg += "_1_";
		new_left->print_log(string(msg + "NEW_LEFT").c_str(), nullptr, left_rec_cnt);
		new_right->print_log(string(msg + "NEW_RIGHT").c_str(), nullptr, right_rec_cnt);
		new_parent->print_log(string(msg + "NEW_PARENT").c_str());
	}

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
	int ret = 0;

	//freeze
	if (!try_freeze())
		return EFROZEN;

	print_log("CONSOLIDATE_BEGIN");

	//申请mdesc
	mdesc_t tmp_mdesc = tree->alloc_mdesc(RELEASE_NEW_ON_FAILED);

	//初始化节点内容为0
	rel_ptr<bz_node<Key, Val>> node = tree->alloc_node<Val>(tmp_mdesc);
	this->copy_node_to(node);
	node->fr_sort_meta();
	//持久化
	persist(node.abs(), NODE_ALLOC_SIZE);

	mdesc_t mdesc = tree->alloc_mdesc();
	rel_ptr<bz_node<Key, Val>> this_node(this);

	//如果需要修改父节点，确保其Frozen != 0
	if (!parent_ptr.is_null()) {
		uint64_t status_rd = pmwcas_read(parent_status.abs());
		if (is_frozen(status_rd)) {
			ret = EFROZEN;
			goto IMMEDIATE_ABORT;
		}
		pmwcas_add(mdesc, parent_status, status_rd, status_rd, 0); 
		pmwcas_add(mdesc, parent_ptr, this_node.rel(), node.rel());
	}
	else {
		pmwcas_add(mdesc, &tree->root_, this_node.rel(), node.rel());
	}

	//执行pmwcas
	if (!pmwcas_commit(mdesc))
		ret = ERACE;
	else
		goto FINISH_HERE;

IMMEDIATE_ABORT:
	pmwcas_word_recycle(node);
	unfreeze();
FINISH_HERE:
	pmwcas_abort(tmp_mdesc);
	pmwcas_free(mdesc);

	print_log("CONSOLIDATE_END", NULL, ret);
	if (!ret) {
		node->print_log("CONSOLIDATE_NEW");
	}

	return ret;
}
/* 计算节点均分时坐节点的数据项个数，单线程调用 */
template<typename Key, typename Val>
uint32_t bz_node<Key, Val>::fr_get_balanced_count(rel_ptr<bz_node<Key, Val>> dst)
{
	uint32_t old_blk_sz = valid_block_size();
	uint64_t * meta_arr = dst->rec_meta_arr();
	uint32_t left_rec_cnt = 0;
	for (uint32_t i = 0, acc_sz = 0; acc_sz < old_blk_sz / 2; ++i) {
		if (is_visiable(meta_arr[i])) {
			++left_rec_cnt;
			acc_sz += get_total_length(meta_arr[i]);
		}
	}
	return left_rec_cnt;
}


template<typename Key, typename Val>
uint32_t bz_node<Key, Val>::copy_node_to(rel_ptr<bz_node<Key, Val>> dst, uint64_t status_rd)
{
	if (!status_rd)
		status_rd = pmwcas_read(&status_);
	uint64_t * meta_arr = rec_meta_arr();
	uint64_t * new_meta_arr = dst->rec_meta_arr();
	uint32_t rec_cnt = get_record_count(status_rd);
	uint32_t new_rec_cnt = get_record_count(dst->status_);
	uint32_t new_blk_sz = get_block_size(dst->status_);
	uint32_t new_node_sz = get_node_size(dst->length_);
	for (uint32_t i = 0; i < rec_cnt; ++i) {
		uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
		if (is_visiable(meta_rd)) {
			const Key * key = get_key(meta_rd);
			const Val * val = get_value(meta_rd);

			assert(*(uint64_t*)key < 65 || *(uint64_t*)key == BZ_KEY_MAX);
			uint64_t tmp = *(uint64_t*)val;
			assert(!(tmp & MwCAS_BIT || tmp & RDCSS_BIT || tmp & DIRTY_BIT));

			uint32_t key_sz = get_key_length(meta_rd);
			uint32_t tot_sz = get_total_length(meta_rd);
			uint32_t offset = new_node_sz - new_blk_sz - tot_sz - 1;
			new_meta_arr[new_rec_cnt] = meta_vis_off_klen_tlen(0, true, offset, key_sz, tot_sz);
			dst->copy_data(offset, key, val, key_sz, tot_sz);
			new_blk_sz += tot_sz;
			++new_rec_cnt;
		}
	}
	set_record_count(dst->status_, new_rec_cnt);
	set_block_size(dst->status_, new_blk_sz);
	if (is_leaf(length_)) {
		set_leaf(dst->length_);
	}
	else {
		set_non_leaf(dst->length_);
	}
	return new_rec_cnt;
}

template<typename Key, typename Val>
void bz_node<Key, Val>::fr_sort_meta()
{
	uint64_t tot_rec_cnt = get_record_count(status_);
	uint64_t * new_meta_arr = rec_meta_arr();
	std::sort(new_meta_arr, new_meta_arr + tot_rec_cnt,
		std::bind(&bz_node<Key, Val>::key_cmp_meta, this, std::placeholders::_1, std::placeholders::_2));
	//计算有效meta数目
	uint32_t new_rec_cnt = binary_search(nullptr, (int)tot_rec_cnt);
	set_sorted_count(length_, new_rec_cnt);
	set_record_count(status_, new_rec_cnt);
}

template<typename Key, typename Val>
inline void bz_node<Key, Val>::fr_remove_meta(int pos)
{
	uint64_t * parent_meta_arr = rec_meta_arr();
	uint32_t rec_cnt = get_record_count(status_);
	uint32_t dele_sz = get_delete_size(status_);
	memmove(parent_meta_arr + pos, parent_meta_arr + pos + 1, (rec_cnt - 1 - pos) * sizeof(uint64_t));
	//增加delete size，减少rec_cnt
	set_record_count(status_, rec_cnt - 1);
	set_delete_size(status_, dele_sz + get_total_length(parent_meta_arr[pos]));
}

template<typename Key, typename Val>
inline int bz_node<Key, Val>::fr_insert_meta(const Key * K, uint64_t left, uint32_t key_sz, uint64_t right)
{
	uint64_t * meta_arr = rec_meta_arr();
	uint32_t tot_sz = key_sz + sizeof(uint64_t);
	uint32_t rec_cnt = get_record_count(status_);
	uint32_t blk_sz = get_block_size(status_);
	uint32_t node_sz = get_node_size(length_);
	uint32_t pos = binary_search(K, rec_cnt);
	if ((rec_cnt + 2) * sizeof(uint64_t) + key_sz + blk_sz + sizeof(*this) > node_sz) {
		return EALLOCSIZE;
	}
	memmove(meta_arr + pos + 1, meta_arr + pos, sizeof(uint64_t) * (rec_cnt - pos));
	uint32_t key_offset = node_sz - blk_sz - tot_sz - 1;
	meta_arr[pos] = meta_vis_off_klen_tlen(0, true, key_offset, key_sz, tot_sz);
	this->copy_data(key_offset, K, &left, key_sz, tot_sz);
	//修改原来指向N的指针，现在指向new_right
	*get_value(meta_arr[pos + 1]) = right;
	assert(!(right & MwCAS_BIT || right & RDCSS_BIT || right & DIRTY_BIT));
	set_record_count(status_, rec_cnt + 1);
	set_sorted_count(length_, rec_cnt + 1);
	set_block_size(status_, blk_sz + tot_sz);
	return 0;
}

template<typename Key, typename Val>
int bz_node<Key, Val>::fr_root_init(const Key * K, uint64_t left, uint32_t key_sz, uint64_t right)
{
	uint64_t * meta_arr = rec_meta_arr();
	uint32_t node_sz = get_node_size(length_);
	uint32_t tot_sz = key_sz + sizeof(uint64_t);
	if (node_sz < sizeof(*this) + sizeof(uint64_t) * 2 - tot_sz * 2)
		return EALLOCSIZE;

	uint32_t left_key_offset = node_sz - tot_sz - 1;
	meta_arr[0] = meta_vis_off_klen_tlen(0, true, left_key_offset, key_sz, tot_sz);
	copy_data(left_key_offset, K, &left, key_sz, tot_sz);

	//往P'插入<BZ_KEY_MAX, new_right>
	uint32_t right_key_offset = left_key_offset - sizeof(uint64_t) * 2;
	meta_arr[1] = meta_vis_off_klen_tlen(0, true, right_key_offset, sizeof(uint64_t), sizeof(uint64_t) * 2);
	*(uint64_t*)get_key(meta_arr[1]) = BZ_KEY_MAX;
	*(uint64_t*)get_value(meta_arr[1]) = right;

	//初始化status和length
	set_record_count(status_, 2);
	set_block_size(status_, tot_sz + sizeof(uint64_t) * 2);
	set_sorted_count(length_, 2);
	set_non_leaf(length_);
	return 0;
}


/* 从当前节点拷贝meta到dst节点，并按照键值和可见性排序，返回现有数据数量 */
template<typename Key, typename Val>
uint32_t bz_node<Key, Val>::copy_sort_meta_to(rel_ptr<bz_node<Key, Val>> dst)
{
	//拷贝meta并按键值排序
	uint64_t * new_meta_arr = dst->rec_meta_arr();
	uint64_t * old_meta_arr = rec_meta_arr();
	uint64_t status_rd = pmwcas_read(&status_);
	uint64_t rec_cnt = get_record_count(status_rd);
	uint32_t new_rec_cnt = get_record_count(dst->status_);
	for (uint32_t i = 0; i < rec_cnt; ++i) {
		uint64_t meta_rd = pmwcas_read(&old_meta_arr[i]);
		if (is_visiable(meta_rd)) {
			new_meta_arr[new_rec_cnt] = meta_rd;
			++new_rec_cnt;
		}
	}
	std::sort(new_meta_arr, new_meta_arr + new_rec_cnt,
		std::bind(&bz_node<Key, Val>::key_cmp_meta, this, std::placeholders::_1, std::placeholders::_2));
	//计算有效meta数目
	return this->binary_search(nullptr, new_rec_cnt, new_meta_arr);
	//将剩余部分置空
	//memset(new_meta_arr + new_rec_cnt, 0, (rec_cnt - new_rec_cnt) * sizeof(uint64_t));
	//return new_rec_cnt;
}
/* 拷贝key-value 并返回操作后的block大小 */
template<typename Key, typename Val>
uint32_t bz_node<Key, Val>::copy_payload_to(rel_ptr<bz_node<Key, Val>> dst, uint32_t new_rec_cnt)
{
	uint32_t blk_sz = 0;
	uint32_t node_sz = NODE_ALLOC_SIZE;
	uint64_t * new_meta_arr = dst->rec_meta_arr();
	for (uint32_t i = 0; i < new_rec_cnt; ++i) {
		const Key * key = get_key(new_meta_arr[i]);
		const Val * val = get_value(new_meta_arr[i]);

		assert(*(uint64_t*)key < 65 || *(uint64_t*)key == BZ_KEY_MAX);
		uint64_t tmp = *(uint64_t*)val;
		assert(!(tmp & MwCAS_BIT || tmp & DIRTY_BIT || tmp & RDCSS_BIT));

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
inline uint32_t bz_node<Key, Val>::valid_block_size(uint64_t status_rd)
{
	if (!status_rd) {
		status_rd = pmwcas_read(&status_);
	}
	uint32_t tot_sz = 0;
	uint64_t * meta_arr = rec_meta_arr();
	uint32_t rec_cnt = get_record_count(status_rd);
	for (uint32_t i = 0; i < rec_cnt; ++i) {
		uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
		if (is_visiable(meta_rd))
			tot_sz += get_total_length(meta_rd);
	}
	return tot_sz;
}

template<typename Key, typename Val>
inline uint32_t bz_node<Key, Val>::valid_node_size(uint64_t status_rd)
{
	if (!status_rd) {
		status_rd = pmwcas_read(&status_);
	}
	uint32_t tot_sz = 0;
	uint32_t tot_rec_cnt = 0;
	uint64_t * meta_arr = rec_meta_arr();
	uint32_t rec_cnt = get_record_count(status_rd);
	for (uint32_t i = 0; i < rec_cnt; ++i) {
		uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
		if (is_visiable(meta_rd)) {
			tot_sz += get_total_length(meta_rd);
			++tot_rec_cnt;
		}
	}
	return sizeof(*this) + sizeof(uint64_t) * tot_rec_cnt + tot_sz;
}

template<typename Key, typename Val>
inline uint32_t bz_node<Key, Val>::valid_record_count(uint64_t status_rd)
{
	if (!status_rd) {
		status_rd = pmwcas_read(&status_);
	}
	uint32_t tot_rec_cnt = 0;
	uint64_t * meta_arr = rec_meta_arr();
	uint32_t rec_cnt = get_record_count(status_rd);
	for (uint32_t i = 0; i < rec_cnt; ++i) {
		uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
		if (is_visiable(meta_rd)) {
			++tot_rec_cnt;
		}
	}
	return tot_rec_cnt;
}

template<typename Key, typename Val>
bool bz_node<Key, Val>::try_freeze()
{
	uint64_t status_rd, status_new;
	/*
	do {
		status_rd = pmwcas_read(&status_);
		status_new = status_frozen(status_rd);
	} while (!is_frozen(status_rd)
		&& status_rd != CAS(&status_, status_new, status_rd));
	return !is_frozen(status_rd);
	*/
	status_rd = pmwcas_read(&status_);
	status_new = status_frozen(status_rd);
	return status_rd == CAS(&status_, status_new, status_rd);
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
	uint64_t addr = *nth_val(n);
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

#ifdef BZ_DEBUG

template<typename Key, typename Val>
void bz_node<Key, Val>::print_log(const char * action, const Key * k, uint64_t ret, bool pr)
{
	if (!pr)
		return;
	mylock.lock();
	fs << "<" << hex << rel_ptr<bz_node<Key, Val>>(this).rel() << "> ";
	if (typeid(Key) == typeid(char)) {
		fs << "[" << this_thread::get_id() << "] " << " ";
		fs << action;
		if (k)
			fs << " key " << k;
		if (ret != -1)
			fs << " : " << ret;
		fs << endl;
	}
	else {
		fs << "[" << this_thread::get_id() << "] ";
		fs << action;
		if (k)
			fs << " key " << *k;
		if (ret != -1)
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
	fs << hex << ptr;
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

template<typename Key, typename Val>
inline mdesc_t bz_tree<Key, Val>::alloc_mdesc(int recycle)
{
	mdesc_t mdesc = pmwcas_alloc(&pool_, recycle);
	while (mdesc.is_null()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		mdesc = pmwcas_alloc(&pool_, recycle);
	}
	return mdesc;
}

template<typename Key, typename Val>
inline void bz_tree<Key, Val>::print_node(uint64_t ptr, int extra)
{
	mylock.lock();
	fs << "extra " << ptr << " [" << this_thread::get_id() << "] ";
	rel_ptr<bz_node<Key, Val>> node(ptr);
	uint64_t status_rd = pmwcas_read(&node->status_);
	uint64_t length = node->length_;
	fs << std::setfill('0') << std::hex << std::setw(16) << status_rd << " ";
	fs << std::setfill('0') << std::hex << std::setw(16) << length << "\n";
	uint64_t * meta_arr = node->rec_meta_arr();
	uint32_t rec_cnt = get_record_count(status_rd);
	for (uint32_t i = 0; i < rec_cnt; ++i) {
		uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
		fs << meta_rd << " ";
		if (is_visiable(meta_rd)) {
			const Key * key = node->get_key(meta_rd);
			if (*(uint64_t*)key != BZ_KEY_MAX) {
				if (typeid(Key) == typeid(char))
					fs << (char*)key << " ";
				else
					fs << *key << " ";
			}
			else {
				fs << "KEY_MAX";
			}
		}
		fs << "\n";
	}
	fs << "\n";
	mylock.unlock();
}

/* 打印树结构，单线程调用 */
template<typename Key, typename Val>
void bz_tree<Key, Val>::print_tree(bool pr)
{
	if (!pr)
		return;
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
template<typename TreeVal>
int bz_node<Key, Val>::insert(bz_tree<Key, TreeVal> * tree, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size, uint32_t alloc_epoch)
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
		if (tree->pack_pmwcas({
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
		uint64_t meta_new_rd = pmwcas_read(&meta_arr[rec_cnt]);
		assert(meta_new_rd == meta_new);

		status_rd = pmwcas_read(&status_);
		if (is_frozen(status_rd)) {
			set_offset(meta_arr[rec_cnt], 0);
			persist(&meta_arr[rec_cnt], sizeof(uint64_t));
			return EFROZEN;
		}

		/* 2-word PMwCAS */
		if (tree->pack_pmwcas({
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
template<typename TreeVal>
int bz_node<Key, Val>::remove(bz_tree<Key, TreeVal> * tree, const Key * key)
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

	print_log("RM-pos", key, pos);

	while (true)
	{
		uint64_t meta_rd = pmwcas_read(&meta_arr[pos]);
		if (!is_visiable(meta_rd)) {
			/* 遭遇其他线程的竞争删除 */
			return ENOTFOUND;
		}

		status_rd = pmwcas_read(&status_);
		if (is_frozen(status_rd))
			return EFROZEN;

		/* 增加delete size */
		uint64_t status_new = status_del(status_rd, get_total_length(meta_rd));

		/* unset visible，offset=0*/
		uint64_t meta_new = meta_vis_off(meta_rd, false, 0);

		/* 2-word PMwCAS */
		if (tree->pack_pmwcas({
			{ &status_, status_rd, status_new },
			{ &meta_arr[pos], meta_rd, meta_new }
			}))
			break;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}

	print_log("RM-finish", key, 0);

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
template<typename TreeVal>
int bz_node<Key, Val>::update(bz_tree<Key, TreeVal> * tree, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size, uint32_t alloc_epoch)
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
		if (tree->pack_pmwcas({
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
		uint64_t meta_new_rd = pmwcas_read(&meta_arr[rec_cnt]);
		assert(meta_new_rd == meta_new);

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
		if (tree->pack_pmwcas({
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
template<typename TreeVal>
int bz_node<Key, Val>::read(bz_tree<Key, TreeVal> * tree, const Key * key, Val * val, uint32_t max_val_size)
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
template<typename TreeVal>
int bz_node<Key, Val>::upsert(bz_tree<Key, TreeVal> * tree, const Key * key, const Val * val, uint32_t key_size, uint32_t total_size, uint32_t alloc_epoch)
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
		if (tree->pack_pmwcas({
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
		uint64_t meta_new_rd = pmwcas_read(&meta_arr[rec_cnt]);
		assert(meta_new_rd == meta_new);

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
			if (tree->pack_pmwcas({
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
			if (tree->pack_pmwcas({
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

	uint32_t bin_beg_pos = binary_search(beg_key, sorted_cnt);
	uint32_t bin_end_pos = end_key ? binary_search(end_key, sorted_cnt) : sorted_cnt;
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
inline int bz_tree<Key, Val>::insert(const Key * key, const Val * val, uint32_t key_size, uint32_t total_size)
{
	return traverse(BZ_ACTION_INSERT, true, key, val, key_size, total_size);
}

template<typename Key, typename Val>
inline int bz_tree<Key, Val>::remove(const Key * key)
{
	return traverse(BZ_ACTION_DELETE, true, key);
}

template<typename Key, typename Val>
inline int bz_tree<Key, Val>::update(const Key * key, const Val * val, uint32_t key_size, uint32_t total_size)
{
	return traverse(BZ_ACTION_UPDATE, true, key, val, key_size, total_size);
}

template<typename Key, typename Val>
inline int bz_tree<Key, Val>::upsert(const Key * key, const Val * val, uint32_t key_size, uint32_t total_size)
{
	return traverse(BZ_ACTION_UPSERT, true, key, val, key_size, total_size);
}

template<typename Key, typename Val>
inline int bz_tree<Key, Val>::read(const Key * key, Val * buffer, uint32_t max_val_size)
{
	return traverse(BZ_ACTION_READ, false, key, nullptr, 0, 0, buffer, max_val_size);
}

template<typename Key, typename Val>
int bz_tree<Key, Val>::new_root() {
	mdesc_t mdesc = alloc_mdesc();

	rel_ptr<rel_ptr<bz_node<Key, Val>>> ptr = pmwcas_reserve<bz_node<Key, Val>>(
		mdesc, (rel_ptr<bz_node<Key, Val>>*)&root_, rel_ptr<bz_node<Key, Val>>::null(),
		RELEASE_NEW_ON_FAILED);

	TX_BEGIN(pop_) {
		pmemobj_tx_add_range_direct(ptr.abs(), sizeof(uint64_t));
		*ptr = pmemobj_tx_alloc(NODE_ALLOC_SIZE, TOID_TYPE_NUM(struct bz_node_fake));
	} TX_END;

	//初始化根节点
	rel_ptr<bz_node<Key, Val>> node = *ptr;
	memset(node.abs(), 0, sizeof(bz_node<Key, Val>));
	set_node_size(node->length_, NODE_ALLOC_SIZE);
	persist(node.abs(), sizeof(bz_node<Key, Val>));

	bool ret = pmwcas_commit(mdesc);
	pmwcas_free(mdesc);
	if (ret) {
		node->print_log("NEW_ROOT");
	}
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
	pos = binary_search(key, sorted_cnt);
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
uint32_t bz_node<Key, Val>::binary_search(const Key * key, int size, uint64_t * meta_arr)
{
	if (!size) {
		size = (int)get_sorted_count(length_);
	}
	if (!meta_arr)
		meta_arr = rec_meta_arr();
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
bool bz_tree<Key, Val>::pack_pmwcas(std::vector<std::tuple<rel_ptr<uint64_t>, uint64_t, uint64_t>> casn)
{
	mdesc_t mdesc = alloc_mdesc();
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

#endif // !BZTREE_H
