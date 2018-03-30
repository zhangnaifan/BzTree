/*
* Copyright (C) 2017, Leo Ma <begeekmyfriend@gmail.com>
*/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <fcntl.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "PMwCAS.h"
#include "bplustree.h"
#include "gc.h"

enum {
	BPLUS_TREE_LEAF,
	BPLUS_TREE_NON_LEAF = 1,
};

enum {
	LEFT_SIBLING,
	RIGHT_SIBLING = 1,
};

#define ADDR_STR_WIDTH 16
#define offset_ptr(node) ((char *) (node.abs()) + sizeof(*node))
#define key(node) ((key_t *)offset_ptr(node))
#define data(node) ((rel_ptr<int> *)(offset_ptr(node) + _max_entries * sizeof(key_t)))
#define sub(node) ((rel_ptr<bplus_node> *)(offset_ptr(node) + (_max_order - 1) * sizeof(key_t)))

static int _block_size;
static int _max_entries;
static int _max_order;
static gc_t * gc;

UCHAR* rel_ptr<bplus_node>::base_address;
UCHAR* rel_ptr<int>::base_address;
PMEMoid rel_ptr<bplus_node>::base_oid;

struct layout_root {
	uint64_t padding;
	pmwcas_pool pool;
	struct bplus_tree tree;
	//bool boot[1024 * 8]; // 8k free blocks
	//uint64_t free_space[1024 * 1024 * 10]; // 80M, max block size = 10k bytes
};
const char * layout_name = "bplustree";
POBJ_LAYOUT_BEGIN(layout_name);
POBJ_LAYOUT_TOID(layout_name, struct bplus_node);
POBJ_LAYOUT_END(layout_name);

static inline int is_leaf(rel_ptr<bplus_node> node)
{
	return node->type == BPLUS_TREE_LEAF;
}

static int key_binary_search(rel_ptr<bplus_node> node, key_t target)
{
	key_t *arr = key(node);
	int len = is_leaf(node) ? node->children : node->children - 1;
	int low = -1;
	int high = len;

	while (low + 1 < high) {
		int mid = low + (high - low) / 2;
		if (target > arr[mid]) {
			low = mid;
		}
		else {
			high = mid;
		}
	}

	if (high >= len || arr[high] != target) {
		return -high - 1;
	}
	else {
		return high;
	}
}

static inline int parent_key_index(rel_ptr<bplus_node> parent, key_t key)
{
	int index = key_binary_search(parent, key);
	return index >= 0 ? index : -index - 2;
}

static inline rel_ptr<bplus_node> cache_refer(struct bplus_tree *tree)
{
	int i;
	for (i = 0; i < MIN_CACHE_NUM; i++) {
		if (!tree->used[i]) {
			tree->used[i] = 1;
			char *buf = tree->caches + _block_size * i;
			return rel_ptr<bplus_node>((uint64_t)buf);
		}
	}
	assert(0);
}

static inline void cache_defer(struct bplus_tree *tree, rel_ptr<bplus_node> node)
{
	/* return the node cache borrowed from */
	char *buf = (char *)node.abs();
	int i = (buf - tree->caches) / _block_size;
	tree->used[i] = 0;
}

static rel_ptr<bplus_node> node_new(struct bplus_tree *tree)
{
	rel_ptr<bplus_node> node = cache_refer(tree);
	node->self.set_null();
	node->parent.set_null();
	node->prev.set_null();
	node->next.set_null();
	node->children = 0;
	return node;
}

static inline rel_ptr<bplus_node> non_leaf_new(struct bplus_tree *tree)
{
	rel_ptr<bplus_node> node = node_new(tree);
	node->type = BPLUS_TREE_NON_LEAF;
	return node;
}

static inline rel_ptr<bplus_node> leaf_new(struct bplus_tree *tree)
{
	rel_ptr<bplus_node> node = node_new(tree);
	node->type = BPLUS_TREE_LEAF;
	return node;
}
int mem, alo;

static rel_ptr<bplus_node> node_fetch(struct bplus_tree *tree, rel_ptr<bplus_node> offset)
{
	if (offset.is_null()) {
		return rel_ptr<bplus_node>::null();
	}

	rel_ptr<bplus_node> node = cache_refer(tree);
	auto beg = clock();
	memcpy(node.abs(), offset.abs(), _block_size);
	mem += clock() - beg;
	//int len = pread(tree->fd, node, _block_size, offset);
	//assert(len == _block_size);
	return node;
}

static rel_ptr<bplus_node> node_seek(struct bplus_tree *tree, rel_ptr<bplus_node> offset)
{
	if (offset.is_null()) {
		return rel_ptr<bplus_node>::null();
	}

	int i;
	for (i = 0; i < MIN_CACHE_NUM; i++) {
		if (!tree->used[i]) {
			char *buf = tree->caches + _block_size * i;
			auto beg = clock();

			memcpy(buf, offset.abs(), _block_size);
			mem += clock() - beg;

			//int len = pread(tree->fd, buf, _block_size, offset);
			//assert(len == _block_size);
			return rel_ptr<bplus_node>((uint64_t)buf);
		}
	}
	assert(0);
}

static inline void node_flush(struct bplus_tree *tree, rel_ptr<bplus_node> node)
{
	/*
	if (!node.is_null()) {
		int len = pwrite(tree->fd, node, _block_size, node->self);
		assert(len == _block_size);
		cache_defer(tree, node);
	}
	*/
	//persist(node.abs(), _block_size);
	auto beg = clock();

	memcpy(node->self.abs(), node.abs(), _block_size);
	mem += clock() - beg;

	cache_defer(tree, node);
}

int constr_bplus_node(PMEMobjpool *pop, void *ptr, void *arg)
{
	bplus_node * node = (bplus_node *)ptr;
	node->type = (int)arg;
	node->children = 0;
	node->next.set_null();
	node->parent.set_null();
	node->prev.set_null();
	node->self = node;
	return 0;
}

static rel_ptr<bplus_node> new_node_append(struct bplus_tree *tree, rel_ptr<bplus_node> node)
{
	/* assign new offset to the new node 
	if (list_empty(&tree->free_blocks)) {
		node->self = tree->file_size;
		tree->file_size += _block_size;
	}
	else {
		struct free_block *block;
		block = list_first_entry(&tree->free_blocks, struct free_block, link);
		list_del(&block->link);
		node->self = block->offset;
		free(block);
	}
	*/
	//auto cache_ptr = node;
	auto beg = clock();
	TOID(struct bplus_node) toid;
	POBJ_ALLOC(tree->pop, &toid, struct bplus_node, _block_size, constr_bplus_node, NULL);
	node->self = toid.oid;
	/*
	TX_BEGIN(tree->pop) {
		node->self = pmemobj_tx_alloc(_block_size, TOID_TYPE_NUM(struct bplus_node));
	} TX_END;
	*/
	alo += clock() - beg;
	return node->self;
}

static void node_delete(struct bplus_tree *tree, rel_ptr<bplus_node> node,
	rel_ptr<bplus_node> left, rel_ptr<bplus_node> right)
{
	if (!left.is_null()) {
		if (!right.is_null()) {
			left->next = right->self;
			right->prev = left->self;
			node_flush(tree, right);
		}
		else {
			left->next.set_null();
		}
		node_flush(tree, left);
	}
	else {
		if (!right.is_null()) {
			right->prev.set_null();
			node_flush(tree, right);
		}
	}
	/*
	assert(!node->self.is_null());
	struct free_block *block = malloc(sizeof(*block));
	assert(block != NULL);

	block->offset = node->self;
	list_add_tail(&block->link, &tree->free_blocks);
	*/

	PMEMoid oid = node->self.oid();
	
	pmemobj_free(&oid);
	/* return the node cache borrowed from */
	cache_defer(tree, node);
}

static inline void sub_node_update(struct bplus_tree *tree, rel_ptr<bplus_node> parent,
	int index, rel_ptr<bplus_node> sub_node)
{
	assert(!sub_node->self.is_null());
	sub(parent)[index] = sub_node->self;
	sub_node->parent = parent->self;
	//node_flush(tree, sub_node);
}

/*
static inline void sub_node_flush(struct bplus_tree *tree, rel_ptr<bplus_node> parent, rel_ptr<bplus_node> sub_offset)
{
	rel_ptr<bplus_node> sub_node = node_fetch(tree, sub_offset);
	assert(!sub_node.is_null());
	sub_node->parent = parent->self;
	node_flush(tree, sub_node);
}
*/

static rel_ptr<int> bplus_tree_search(struct bplus_tree *tree, key_t key)
{
	rel_ptr<int> ret;
	ret.set_null();
	rel_ptr<bplus_node> node = tree->root;
	while (!node.is_null()) {
		int i = key_binary_search(node, key);
		if (is_leaf(node)) {
			ret = i >= 0 ? data(node)[i] : rel_ptr<int>::null();
			break;
		}
		else {
			if (i >= 0) {
				node = sub(node)[i + 1];
			}
			else {
				i = -i - 1;
				node = sub(node)[i];
			}
		}
	}

	return ret;
}

static void left_node_add(struct bplus_tree *tree, rel_ptr<bplus_node> node, rel_ptr<bplus_node> left)
{
	rel_ptr<bplus_node> prev = node->prev;
	if (!prev.is_null()) {
		prev->next = left->self;
		left->prev = prev->self;
	}
	else {
		left->prev.set_null();
	}
	left->next = node->self;
	node->prev = left->self;
}

static void right_node_add(struct bplus_tree *tree, rel_ptr<bplus_node> node, rel_ptr<bplus_node> right)
{
	//new_node_append(tree, right);
	rel_ptr<bplus_node> next = node->next;
	auto tmp = *next;
	if (!next.is_null()) {
		next->prev = right->self;
		right->next = next->self;
	}
	else {
		right->next.set_null();
	}
	right->prev = node->self;
	node->next = right->self;
}

static key_t non_leaf_insert(struct bplus_tree *tree, rel_ptr<bplus_node> node,
	rel_ptr<bplus_node> l_ch, rel_ptr<bplus_node> r_ch, key_t key);

rel_ptr<bplus_node> malloc_node(bplus_tree * tree, int type)
{
	TOID(struct bplus_node) toid;
	POBJ_ALLOC(tree->pop, &toid, struct bplus_node, _block_size, constr_bplus_node, (void*) type);
	return toid.oid;
}

static int parent_node_build(struct bplus_tree *tree, 
	rel_ptr<bplus_node> l_ch, rel_ptr<bplus_node> r_ch, key_t key)
{
	if (l_ch->parent.is_null() && r_ch->parent.is_null()) {
		/* new parent */
		rel_ptr<bplus_node> parent = malloc_node(tree, BPLUS_TREE_NON_LEAF);
		key(parent)[0] = key;
		sub(parent)[0] = l_ch->self;
		sub(parent)[1] = r_ch->self;
		parent->children = 2;
		/* write new parent and update root */
		tree->root = parent;

		l_ch->parent = parent->self;
		r_ch->parent = parent->self;
		tree->level++;

		/* flush parent, left and right child */
		//node_flush(tree, l_ch);
		//node_flush(tree, r_ch);
		//node_flush(tree, parent);
		return 0;
	}
	else if (r_ch->parent.is_null()) {
		return non_leaf_insert(tree, l_ch->parent, l_ch, r_ch, key);
	}
	else {
		return non_leaf_insert(tree, r_ch->parent, l_ch, r_ch, key);
	}
}

static key_t non_leaf_split_left(struct bplus_tree *tree, rel_ptr<bplus_node> node,
	rel_ptr<bplus_node> l_ch,
	rel_ptr<bplus_node> r_ch, key_t key, int insert)
{
	int i;
	key_t split_key;

	/* split = [m/2] */
	int split = (_max_order + 1) / 2;

	/* split as left sibling */
	//new_node_append(tree, left);
	rel_ptr<bplus_node> left = malloc_node(tree, BPLUS_TREE_NON_LEAF);

	left_node_add(tree, node, left);

	/* calculate split nodes' children (sum as (order + 1))*/
	int pivot = insert;
	left->children = split;
	node->children = _max_order - split + 1;

	/* sum = left->children = pivot + (split - pivot - 1) + 1 */
	/* replicate from key[0] to key[insert] in original node */
	memmove(&key(left)[0], &key(node)[0], pivot * sizeof(key_t));
	memmove(&sub(left)[0], &sub(node)[0], pivot * sizeof(rel_ptr<bplus_node>));

	/* replicate from key[insert] to key[split - 1] in original node */
	memmove(&key(left)[pivot + 1], &key(node)[pivot], (split - pivot - 1) * sizeof(key_t));
	memmove(&sub(left)[pivot + 1], &sub(node)[pivot], (split - pivot - 1) * sizeof(rel_ptr<bplus_node>));

	/* flush sub-nodes of the new splitted left node */
	for (i = 0; i < left->children; i++) {
		if (i != pivot && i != pivot + 1) {
			sub(left)[i]->parent = left->self;
		}
	}

	/* insert new key and sub-nodes and locate the split key */
	key(left)[pivot] = key;
	if (pivot == split - 1) {
		/* left child in split left node and right child in original right one */
		sub_node_update(tree, left, pivot, l_ch);
		sub_node_update(tree, node, 0, r_ch);
		split_key = key;
	}
	else {
		/* both new children in split left node */
		sub_node_update(tree, left, pivot, l_ch);
		sub_node_update(tree, left, pivot + 1, r_ch);
		sub(node)[0] = sub(node)[split - 1];
		split_key = key(node)[split - 2];
	}

	/* sum = node->children = 1 + (node->children - 1) */
	/* right node left shift from key[split - 1] to key[children - 2] */
	memmove(&key(node)[0], &key(node)[split - 1], (node->children - 1) * sizeof(key_t));
	memmove(&sub(node)[1], &sub(node)[split], (node->children - 1) * sizeof(rel_ptr<bplus_node>));

	return split_key;
}

static key_t non_leaf_split_right1(struct bplus_tree *tree, rel_ptr<bplus_node> node,
	rel_ptr<bplus_node> l_ch,
	rel_ptr<bplus_node> r_ch, key_t key, int insert)
{
	int i;

	/* split = [m/2] */
	int split = (_max_order + 1) / 2;

	/* split as right sibling */
	rel_ptr<bplus_node> right = malloc_node(tree, BPLUS_TREE_NON_LEAF);
	right_node_add(tree, node, right);

	/* split key is key[split - 1] */
	key_t split_key = key(node)[split - 1];

	/* calculate split nodes' children (sum as (order + 1))*/
	int pivot = 0;
	node->children = split;
	right->children = _max_order - split + 1;

	/* insert new key and sub-nodes */
	key(right)[0] = key;
	sub_node_update(tree, right, pivot, l_ch);
	sub_node_update(tree, right, pivot + 1, r_ch);

	/* sum = right->children = 2 + (right->children - 2) */
	/* replicate from key[split] to key[_max_order - 2] */
	memmove(&key(right)[pivot + 1], &key(node)[split], (right->children - 2) * sizeof(key_t));
	memmove(&sub(right)[pivot + 2], &sub(node)[split + 1], (right->children - 2) * sizeof(rel_ptr<bplus_node>));

	/* flush sub-nodes of the new splitted right node */
	for (i = pivot + 2; i < right->children; i++) {
		sub(right)[i]->parent = right->self;
	}

	return split_key;
}

static key_t non_leaf_split_right2(struct bplus_tree *tree, rel_ptr<bplus_node> node,
	rel_ptr<bplus_node> l_ch,
	rel_ptr<bplus_node> r_ch, key_t key, int insert)
{
	int i;

	/* split = [m/2] */
	int split = (_max_order + 1) / 2;

	/* split as right sibling */
	rel_ptr<bplus_node> right = malloc_node(tree, BPLUS_TREE_NON_LEAF);
	right_node_add(tree, node, right);

	/* split key is key[split] */
	key_t split_key = key(node)[split];

	/* calculate split nodes' children (sum as (order + 1))*/
	int pivot = insert - split - 1;
	node->children = split + 1;
	right->children = _max_order - split;

	/* sum = right->children = pivot + 2 + (_max_order - insert - 1) */
	/* replicate from key[split + 1] to key[insert] */
	memmove(&key(right)[0], &key(node)[split + 1], pivot * sizeof(key_t));
	memmove(&sub(right)[0], &sub(node)[split + 1], pivot * sizeof(rel_ptr<bplus_node>));

	/* insert new key and sub-node */
	key(right)[pivot] = key;
	sub_node_update(tree, right, pivot, l_ch);
	sub_node_update(tree, right, pivot + 1, r_ch);

	/* replicate from key[insert] to key[order - 1] */
	memmove(&key(right)[pivot + 1], &key(node)[insert], (_max_order - insert - 1) * sizeof(key_t));
	memmove(&sub(right)[pivot + 2], &sub(node)[insert + 1], (_max_order - insert - 1) * sizeof(rel_ptr<bplus_node>));

	/* flush sub-nodes of the new splitted right node */
	for (i = 0; i < right->children; i++) {
		if (i != pivot && i != pivot + 1) {
			sub(right)[i]->parent = right->self;
		}
	}

	return split_key;
}

static void non_leaf_simple_insert(struct bplus_tree *tree, rel_ptr<bplus_node> node,
	rel_ptr<bplus_node> l_ch, rel_ptr<bplus_node> r_ch,
	key_t key, int insert)
{
	memmove(&key(node)[insert + 1], &key(node)[insert], (node->children - 1 - insert) * sizeof(key_t));
	memmove(&sub(node)[insert + 2], &sub(node)[insert + 1], (node->children - 1 - insert) * sizeof(rel_ptr<bplus_node>));
	/* insert new key and sub-nodes */
	key(node)[insert] = key;
	sub_node_update(tree, node, insert, l_ch);
	sub_node_update(tree, node, insert + 1, r_ch);
	node->children++;
}

static int non_leaf_insert(struct bplus_tree *tree, rel_ptr<bplus_node> node,
	rel_ptr<bplus_node> l_ch, rel_ptr<bplus_node> r_ch, key_t key)
{
	/* Search key location */
	int insert = key_binary_search(node, key);
	assert(insert < 0);
	insert = -insert - 1;

	/* node is full */
	if (node->children == _max_order) {
		key_t split_key;
		/* split = [m/2] */
		int split = (node->children + 1) / 2;
		//rel_ptr<bplus_node> sibling = non_leaf_new(tree);
		if (insert < split) {
			split_key = non_leaf_split_left(tree, node, l_ch, r_ch, key, insert);
		}
		else if (insert == split) {
			split_key = non_leaf_split_right1(tree, node, l_ch, r_ch, key, insert);
		}
		else {
			split_key = non_leaf_split_right2(tree, node, l_ch, r_ch, key, insert);
		}

		/* build new parent */
		if (insert < split) {
			return parent_node_build(tree, node->prev, node, split_key);
		}
		else {
			return parent_node_build(tree, node, node->next, split_key);
		}
	}
	else {
		non_leaf_simple_insert(tree, node, l_ch, r_ch, key, insert);
		//node_flush(tree, node);
	}
	return 0;
}

static key_t leaf_split_left(struct bplus_tree *tree, rel_ptr<bplus_node> leaf,
	key_t key, rel_ptr<int> data, int insert)
{
	/* split = [m/2] */
	int split = (leaf->children + 1) / 2;

	/* split as left sibling */
	rel_ptr<bplus_node> left;
	left_node_add(tree, leaf, left);

	/* calculate split leaves' children (sum as (entries + 1)) */
	int pivot = insert;
	left->children = split;
	leaf->children = _max_entries - split + 1;

	/* sum = left->children = pivot + 1 + (split - pivot - 1) */
	/* replicate from key[0] to key[insert] */
	memmove(&key(left)[0], &key(leaf)[0], pivot * sizeof(key_t));
	memmove(&data(left)[0], &data(leaf)[0], pivot * sizeof(rel_ptr<int>));

	/* insert new key and data */
	key(left)[pivot] = key;
	data(left)[pivot] = data;

	/* replicate from key[insert] to key[split - 1] */
	memmove(&key(left)[pivot + 1], &key(leaf)[pivot], (split - pivot - 1) * sizeof(key_t));
	memmove(&data(left)[pivot + 1], &data(leaf)[pivot], (split - pivot - 1) * sizeof(rel_ptr<int>));

	/* original leaf left shift */
	memmove(&key(leaf)[0], &key(leaf)[split - 1], leaf->children * sizeof(key_t));
	memmove(&data(leaf)[0], &data(leaf)[split - 1], leaf->children * sizeof(rel_ptr<int>));

	return key(leaf)[0];
}

static key_t leaf_split_right(struct bplus_tree *tree, rel_ptr<bplus_node> leaf,
	key_t key, rel_ptr<int> data, int insert)
{
	/* split = [m/2] */
	int split = (leaf->children + 1) / 2;

	/* split as right sibling */
	rel_ptr<bplus_node> right = malloc_node(tree, BPLUS_TREE_LEAF);
	right_node_add(tree, leaf, right);

	/* calculate split leaves' children (sum as (entries + 1)) */
	int pivot = insert - split;
	leaf->children = split;
	right->children = _max_entries - split + 1;

	/* sum = right->children = pivot + 1 + (_max_entries - pivot - split) */
	/* replicate from key[split] to key[children - 1] in original leaf */
	memmove(&key(right)[0], &key(leaf)[split], pivot * sizeof(key_t));
	memmove(&data(right)[0], &data(leaf)[split], pivot * sizeof(rel_ptr<int>));

	/* insert new key and data */
	key(right)[pivot] = key;
	data(right)[pivot] = data;

	/* replicate from key[insert] to key[children - 1] in original leaf */
	memmove(&key(right)[pivot + 1], &key(leaf)[insert], (_max_entries - insert) * sizeof(key_t));
	memmove(&data(right)[pivot + 1], &data(leaf)[insert], (_max_entries - insert) * sizeof(rel_ptr<int>));

	return key(right)[0];
}

static void leaf_simple_insert(struct bplus_tree *tree, rel_ptr<bplus_node> leaf,
	key_t key, rel_ptr<int> data, int insert)
{
	memmove(&key(leaf)[insert + 1], &key(leaf)[insert], (leaf->children - insert) * sizeof(key_t));
	memmove(&data(leaf)[insert + 1], &data(leaf)[insert], (leaf->children - insert) * sizeof(rel_ptr<int>));
	key(leaf)[insert] = key;
	data(leaf)[insert] = data;
	leaf->children++;
}

static int leaf_insert(struct bplus_tree *tree, rel_ptr<bplus_node> leaf, key_t key, rel_ptr<int> data)
{
	/* Search key location */
	int insert = key_binary_search(leaf, key);
	if (insert >= 0) {
		/* Already exists */
		return -1;
	}
	insert = -insert - 1;

	/* leaf is full */
	if (leaf->children == _max_entries) {
		key_t split_key;
		/* split = [m/2] */
		int split = (_max_entries + 1) / 2;

		/* sibling leaf replication due to location of insertion */
		if (insert < split) {
			split_key = leaf_split_left(tree, leaf, key, data, insert);
		}
		else {
			split_key = leaf_split_right(tree, leaf, key, data, insert);
		}

		/* build new parent */
		if (insert < split) {
			return parent_node_build(tree, leaf->prev, leaf, split_key);
		}
		else {
			return parent_node_build(tree, leaf, leaf->next, split_key);
		}
	}
	else {
		leaf_simple_insert(tree, leaf, key, data, insert);
		//node_flush(tree, leaf);
	}

	return 0;
}

static int bplus_tree_insert(struct bplus_tree *tree, key_t key, rel_ptr<int> data)
{
	rel_ptr<bplus_node> node = tree->root;
	while (!node.is_null()) {
		if (is_leaf(node)) {
			return leaf_insert(tree, node, key, data);
		}
		else {
			int i = key_binary_search(node, key);
			if (i >= 0) {
				node = sub(node)[i + 1];
			}
			else {
				i = -i - 1;
				node = sub(node)[i];
			}
		}
	}

	/* new root */
	rel_ptr<bplus_node> root = malloc_node(tree, BPLUS_TREE_LEAF);
	key(root)[0] = key;
	data(root)[0] = data;
	root->children = 1;
	tree->root = root;
	tree->level = 1;
	//node_flush(tree, root);
	return 0;
}

static inline int sibling_select(rel_ptr<bplus_node> l_sib, rel_ptr<bplus_node> r_sib,
	rel_ptr<bplus_node> parent, int i)
{
	if (i == -1) {
		/* the frist sub-node, no left sibling, choose the right one */
		return RIGHT_SIBLING;
	}
	else if (i == parent->children - 2) {
		/* the last sub-node, no right sibling, choose the left one */
		return LEFT_SIBLING;
	}
	else {
		/* if both left and right sibling found, choose the one with more children */
		return l_sib->children >= r_sib->children ? LEFT_SIBLING : RIGHT_SIBLING;
	}
}

static void non_leaf_shift_from_left(struct bplus_tree *tree, rel_ptr<bplus_node> node,
	rel_ptr<bplus_node> left, rel_ptr<bplus_node> parent,
	int parent_key_index, int remove)
{
	/* node's elements right shift */
	memmove(&key(node)[1], &key(node)[0], remove * sizeof(key_t));
	memmove(&sub(node)[1], &sub(node)[0], (remove + 1) * sizeof(rel_ptr<bplus_node>));

	/* parent key right rotation */
	key(node)[0] = key(parent)[parent_key_index];
	key(parent)[parent_key_index] = key(left)[left->children - 2];

	/* borrow the last sub-node from left sibling */
	sub(node)[0] = sub(left)[left->children - 1];
	sub(node)[0]->parent = node->self;

	left->children--;
}

static void non_leaf_merge_into_left(struct bplus_tree *tree, rel_ptr<bplus_node> node,
	rel_ptr<bplus_node> left, rel_ptr<bplus_node> parent,
	int parent_key_index, int remove)
{
	/* move parent key down */
	key(left)[left->children - 1] = key(parent)[parent_key_index];

	/* merge into left sibling */
	/* key sum = node->children - 2 */
	memmove(&key(left)[left->children], &key(node)[0], remove * sizeof(key_t));
	memmove(&sub(left)[left->children], &sub(node)[0], (remove + 1) * sizeof(rel_ptr<bplus_node>));

	/* sub-node sum = node->children - 1 */
	memmove(&key(left)[left->children + remove], &key(node)[remove + 1], (node->children - remove - 2) * sizeof(key_t));
	memmove(&sub(left)[left->children + remove + 1], &sub(node)[remove + 2], (node->children - remove - 2) * sizeof(rel_ptr<bplus_node>));

	/* flush sub-nodes of the new merged left node */
	int i, j;
	for (i = left->children, j = 0; j < node->children - 1; i++, j++) {
		sub(left)[i]->parent = left->self;
	}

	left->children += node->children - 1;
}

static void non_leaf_shift_from_right(struct bplus_tree *tree, rel_ptr<bplus_node> node,
	rel_ptr<bplus_node> right, rel_ptr<bplus_node> parent,
	int parent_key_index)
{
	/* parent key left rotation */
	key(node)[node->children - 1] = key(parent)[parent_key_index];
	key(parent)[parent_key_index] = key(right)[0];

	/* borrow the frist sub-node from right sibling */
	sub(node)[node->children] = sub(right)[0];
	sub(node)[node->children]->parent = node->self;

	node->children++;

	/* right sibling left shift*/
	memmove(&key(right)[0], &key(right)[1], (right->children - 2) * sizeof(key_t));
	memmove(&sub(right)[0], &sub(right)[1], (right->children - 1) * sizeof(rel_ptr<bplus_node>));

	right->children--;
}

static void non_leaf_merge_from_right(struct bplus_tree *tree, rel_ptr<bplus_node> node,
	rel_ptr<bplus_node> right, rel_ptr<bplus_node> parent,
	int parent_key_index)
{
	/* move parent key down */
	key(node)[node->children - 1] = key(parent)[parent_key_index];
	node->children++;

	/* merge from right sibling */
	memmove(&key(node)[node->children - 1], &key(right)[0], (right->children - 1) * sizeof(key_t));
	memmove(&sub(node)[node->children - 1], &sub(right)[0], right->children * sizeof(rel_ptr<bplus_node>));

	/* flush sub-nodes of the new merged node */
	int i, j;
	for (i = node->children - 1, j = 0; j < right->children; i++, j++) {
		sub(node)[i]->parent = node->self;

	}

	node->children += right->children - 1;
}

static inline void non_leaf_simple_remove(struct bplus_tree *tree, rel_ptr<bplus_node> node, int remove)
{
	assert(node->children >= 2);
	memmove(&key(node)[remove], &key(node)[remove + 1], (node->children - remove - 2) * sizeof(key_t));
	memmove(&sub(node)[remove + 1], &sub(node)[remove + 2], (node->children - remove - 2) * sizeof(rel_ptr<bplus_node>));
	node->children--;
}

static void non_leaf_remove(struct bplus_tree *tree, rel_ptr<bplus_node> node, int remove)
{
	if (node->parent.is_null()) {
		/* node is the root */
		if (node->children == 2) {
			/* replace old root with the first sub-node */
			rel_ptr<bplus_node> root = node_fetch(tree, sub(node)[0]);
			root->parent.set_null();
			tree->root = root->self;
			tree->level--;
			node_delete(tree, node, rel_ptr<bplus_node>::null(), rel_ptr<bplus_node>::null());
			node_flush(tree, root);
		}
		else {
			non_leaf_simple_remove(tree, node, remove);
			node_flush(tree, node);
		}
	}
	else if (node->children <= (_max_order + 1) / 2) {
		rel_ptr<bplus_node> l_sib = node_fetch(tree, node->prev);
		rel_ptr<bplus_node> r_sib = node_fetch(tree, node->next);
		rel_ptr<bplus_node> parent = node_fetch(tree, node->parent);

		int i = parent_key_index(parent, key(node)[0]);

		/* decide which sibling to be borrowed from */
		if (sibling_select(l_sib, r_sib, parent, i) == LEFT_SIBLING) {
			if (l_sib->children > (_max_order + 1) / 2) {
				non_leaf_shift_from_left(tree, node, l_sib, parent, i, remove);
				/* flush nodes */
				node_flush(tree, node);
				node_flush(tree, l_sib);
				node_flush(tree, r_sib);
				node_flush(tree, parent);
			}
			else {
				non_leaf_merge_into_left(tree, node, l_sib, parent, i, remove);
				/* delete empty node and flush */
				node_delete(tree, node, l_sib, r_sib);
				/* trace upwards */
				non_leaf_remove(tree, parent, i);
			}
		}
		else {
			/* remove at first in case of overflow during merging with sibling */
			non_leaf_simple_remove(tree, node, remove);

			if (r_sib->children > (_max_order + 1) / 2) {
				non_leaf_shift_from_right(tree, node, r_sib, parent, i + 1);
				/* flush nodes */
				node_flush(tree, node);
				node_flush(tree, l_sib);
				node_flush(tree, r_sib);
				node_flush(tree, parent);
			}
			else {
				non_leaf_merge_from_right(tree, node, r_sib, parent, i + 1);
				/* delete empty right sibling and flush */
				rel_ptr<bplus_node> rr_sib = node_fetch(tree, r_sib->next);
				node_delete(tree, r_sib, node, rr_sib);
				node_flush(tree, l_sib);
				/* trace upwards */
				non_leaf_remove(tree, parent, i + 1);
			}
		}
	}
	else {
		non_leaf_simple_remove(tree, node, remove);
		node_flush(tree, node);
	}
}

static void leaf_shift_from_left(struct bplus_tree *tree, rel_ptr<bplus_node> leaf,
	rel_ptr<bplus_node> left, rel_ptr<bplus_node> parent,
	int parent_key_index, int remove)
{
	/* right shift in leaf node */
	memmove(&key(leaf)[1], &key(leaf)[0], remove * sizeof(key_t));
	memmove(&data(leaf)[1], &data(leaf)[0], remove * sizeof(rel_ptr<bplus_node>));

	/* borrow the last element from left sibling */
	key(leaf)[0] = key(left)[left->children - 1];
	data(leaf)[0] = data(left)[left->children - 1];
	left->children--;

	/* update parent key */
	key(parent)[parent_key_index] = key(leaf)[0];
}

static void leaf_merge_into_left(struct bplus_tree *tree, rel_ptr<bplus_node> leaf,
	rel_ptr<bplus_node> left, int parent_key_index, int remove)
{
	/* merge into left sibling, sum = leaf->children - 1*/
	memmove(&key(left)[left->children], &key(leaf)[0], remove * sizeof(key_t));
	memmove(&data(left)[left->children], &data(leaf)[0], remove * sizeof(rel_ptr<bplus_node>));
	memmove(&key(left)[left->children + remove], &key(leaf)[remove + 1], (leaf->children - remove - 1) * sizeof(key_t));
	memmove(&data(left)[left->children + remove], &data(leaf)[remove + 1], (leaf->children - remove - 1) * sizeof(rel_ptr<bplus_node>));
	left->children += leaf->children - 1;
}

static void leaf_shift_from_right(struct bplus_tree *tree, rel_ptr<bplus_node> leaf,
	rel_ptr<bplus_node> right, rel_ptr<bplus_node> parent,
	int parent_key_index)
{
	/* borrow the first element from right sibling */
	key(leaf)[leaf->children] = key(right)[0];
	data(leaf)[leaf->children] = data(right)[0];
	leaf->children++;

	/* left shift in right sibling */
	memmove(&key(right)[0], &key(right)[1], (right->children - 1) * sizeof(key_t));
	memmove(&data(right)[0], &data(right)[1], (right->children - 1) * sizeof(rel_ptr<bplus_node>));
	right->children--;

	/* update parent key */
	key(parent)[parent_key_index] = key(right)[0];
}

static inline void leaf_merge_from_right(struct bplus_tree *tree, rel_ptr<bplus_node> leaf,
	rel_ptr<bplus_node> right)
{
	memmove(&key(leaf)[leaf->children], &key(right)[0], right->children * sizeof(key_t));
	memmove(&data(leaf)[leaf->children], &data(right)[0], right->children * sizeof(rel_ptr<bplus_node>));
	leaf->children += right->children;
}

static inline void leaf_simple_remove(struct bplus_tree *tree, rel_ptr<bplus_node> leaf, int remove)
{
	memmove(&key(leaf)[remove], &key(leaf)[remove + 1], (leaf->children - remove - 1) * sizeof(key_t));
	memmove(&data(leaf)[remove], &data(leaf)[remove + 1], (leaf->children - remove - 1) * sizeof(rel_ptr<bplus_node>));
	leaf->children--;
}

static int leaf_remove(struct bplus_tree *tree, rel_ptr<bplus_node> leaf, key_t key)
{
	int remove = key_binary_search(leaf, key);
	if (remove < 0) {
		/* Not exist */
		return -1;
	}

	/* fetch from free node caches */
	int i = ((char *)leaf.abs() - tree->caches) / _block_size;
	tree->used[i] = 1;

	if (leaf->parent.is_null()) {
		/* leaf as the root */
		if (leaf->children == 1) {
			/* delete the only last node */
			assert(key == key(leaf)[0]);
			tree->root.set_null();
			tree->level = 0;
			node_delete(tree, leaf, rel_ptr<bplus_node>::null(), rel_ptr<bplus_node>::null());
		}
		else {
			leaf_simple_remove(tree, leaf, remove);
			node_flush(tree, leaf);
		}
	}
	else if (leaf->children <= (_max_entries + 1) / 2) {
		rel_ptr<bplus_node> l_sib = node_fetch(tree, leaf->prev);
		rel_ptr<bplus_node> r_sib = node_fetch(tree, leaf->next);
		auto tmp = *r_sib;
		rel_ptr<bplus_node> parent = node_fetch(tree, leaf->parent);

		i = parent_key_index(parent, key(leaf)[0]);

		/* decide which sibling to be borrowed from */
		if (sibling_select(l_sib, r_sib, parent, i) == LEFT_SIBLING) {
			if (l_sib->children > (_max_entries + 1) / 2) {
				leaf_shift_from_left(tree, leaf, l_sib, parent, i, remove);
				/* flush leaves */
				node_flush(tree, leaf);
				node_flush(tree, l_sib);
				node_flush(tree, r_sib);
				node_flush(tree, parent);
			}
			else {
				leaf_merge_into_left(tree, leaf, l_sib, i, remove);
				/* delete empty leaf and flush */
				node_delete(tree, leaf, l_sib, r_sib);
				/* trace upwards */
				non_leaf_remove(tree, parent, i);
			}
		}
		else {
			/* remove at first in case of overflow during merging with sibling */
			leaf_simple_remove(tree, leaf, remove);

			if (r_sib->children > (_max_entries + 1) / 2) {
				leaf_shift_from_right(tree, leaf, r_sib, parent, i + 1);
				/* flush leaves */
				node_flush(tree, leaf);
				node_flush(tree, l_sib);
				node_flush(tree, r_sib);
				node_flush(tree, parent);
			}
			else {
				leaf_merge_from_right(tree, leaf, r_sib);
				/* delete empty right sibling flush */
				rel_ptr<bplus_node> rr_sib = node_fetch(tree, r_sib->next);
				node_delete(tree, r_sib, leaf, rr_sib);
				node_flush(tree, l_sib);
				/* trace upwards */
				non_leaf_remove(tree, parent, i + 1);
			}
		}
	}
	else {
		leaf_simple_remove(tree, leaf, remove);
		node_flush(tree, leaf);
	}

	return 0;
}

static int bplus_tree_delete(struct bplus_tree *tree, key_t key)
{
	rel_ptr<bplus_node> node = tree->root;
	while (!node.is_null()) {
		if (is_leaf(node)) {
			node = node_seek(tree, node);
			return leaf_remove(tree, node, key);
		}
		else {
			int i = key_binary_search(node, key);
			if (i >= 0) {
				node = sub(node)[i + 1];
			}
			else {
				i = -i - 1;
				node = sub(node)[i];
			}
		}
	}
	return -1;
}

rel_ptr<int> bplus_tree_get(struct bplus_tree *tree, key_t key)
{
	return bplus_tree_search(tree, key);
}

int bplus_tree_put(struct bplus_tree *tree, key_t key, rel_ptr<int> data)
{
	if (!data.is_null()) {
		return bplus_tree_insert(tree, key, data);
	}
	else {
		return bplus_tree_delete(tree, key);
	}
}

rel_ptr<int> bplus_tree_get_range(struct bplus_tree *tree, key_t key1, key_t key2)
{
	rel_ptr<int> start;
	start.set_null();
	key_t min = key1 <= key2 ? key1 : key2;
	key_t max = min == key1 ? key2 : key1;

	rel_ptr<bplus_node> node = tree->root;
	while (!node.is_null()) {
		int i = key_binary_search(node, min);
		if (is_leaf(node)) {
			if (i < 0) {
				i = -i - 1;
				if (i >= node->children) {
					node = node->next;
				}
			}
			while (!node.is_null() && key(node)[i] <= max) {
				start = data(node)[i];
				if (++i >= node->children) {
					node = node->next;
					i = 0;
				}
			}
			break;
		}
		else {
			if (i >= 0) {
				node = sub(node)[i + 1];
			}
			else {
				i = -i - 1;
				node = sub(node)[i];
			}
		}
	}

	return start;
}

/*
int bplus_open(char *filename)
{
	return open(filename, O_CREAT | O_RDWR, 0644);
}

void bplus_close(int fd)
{
	close(fd);
}

static rel_ptr<bplus_node> str_to_hex(char *c, int len)
{
	rel_ptr<bplus_node> offset = 0;
	while (len-- > 0) {
		if (isdigit(*c)) {
			offset = offset * 16 + *c - '0';
		}
		else if (isxdigit(*c)) {
			if (islower(*c)) {
				offset = offset * 16 + *c - 'a' + 10;
			}
			else {
				offset = offset * 16 + *c - 'A' + 10;
			}
		}
		c++;
	}
	return offset;
}

static inline void hex_to_str(rel_ptr<bplus_node> offset, char *buf, int len)
{
	const static char *hex = "0123456789ABCDEF";
	while (len-- > 0) {
		buf[len] = hex[offset & 0xf];
		offset >>= 4;
	}
}

static inline rel_ptr<bplus_node> offset_load(int fd)
{
	char buf[ADDR_STR_WIDTH];
	ssize_t len = read(fd, buf, sizeof(buf));
	return len > 0 ? str_to_hex(buf, sizeof(buf)) : INVALID_OFFSET;
}

static inline ssize_t offset_store(int fd, rel_ptr<bplus_node> offset)
{
	char buf[ADDR_STR_WIDTH];
	hex_to_str(offset, buf, sizeof(buf));
	return write(fd, buf, sizeof(buf));
}
*/


struct bplus_tree* first_use(int block_size = 1024)
{
	remove("dbstore.pool");
	PMEMobjpool *pop = pmemobj_createU("dbstore.pool", "layout",
		PMEMOBJ_MIN_POOL + sizeof(layout_root) + 10 * 1024 * 1024, 0666);
	struct layout_root * root = (layout_root *)pmemobj_direct(pmemobj_root(pop, sizeof(layout_root)));
	assert(pop && root);

	mdesc_pool_t pool = &root->pool;
	struct bplus_tree * tree = &root->tree;

	/*
	if ((block_size & (block_size - 1)) != 0 || block_size > 1024 * 10) {
		fprintf(stderr, "Block size must be pow of 2 and less than 10k bytes!\n");
		return NULL;
	}
	*/

	if (block_size < (int) sizeof(bplus_node)) {
		fprintf(stderr, "block size is too small for one node!\n");
		return NULL;
	}
	_max_order = (block_size - sizeof(bplus_node)) / (sizeof(key_t) + sizeof(rel_ptr<bplus_node>));
	if (_max_order <= 2) {
		fprintf(stderr, "block size is too small for one node!\n");
		return NULL;
	}

	tree->block_size = block_size;
	tree->root.set_null();
	tree->level = 0;
	persist(tree, sizeof(tree));

	pmwcas_first_use(pool);
	pmemobj_close(pop);
	return tree;
}

struct bplus_tree * bplus_tree_init()
{
	PMEMobjpool * pop = pmemobj_openU("dbstore.pool", "layout");
	PMEMoid oid_root = pmemobj_root(pop, sizeof(layout_root));
	struct layout_root * root = (layout_root *)pmemobj_direct(oid_root);
	
	assert(pop && root);
	pmwcas_init(oid_root);
	pmwcas_recovery(&root->pool);

	//gc = gc_create(offsetof(pmwcas_entry, gc_entry), pmwcas_reclaim, gc);

	struct bplus_tree * tree = &root->tree;

	rel_ptr<bplus_node>::set_base(oid_root);
	pmwcas_init(oid_root);
	tree->pop = pop;

	_block_size = tree->block_size;
	_max_order = (_block_size - sizeof(bplus_node)) / (sizeof(key_t) + sizeof(rel_ptr<bplus_node>));
	_max_entries = (_block_size - sizeof(bplus_node)) / (sizeof(key_t) + sizeof(rel_ptr<int>));

	tree->caches = (char*)malloc(_block_size * MIN_CACHE_NUM);
	for (off_t i = 0; i < MIN_CACHE_NUM; ++i)
		tree->used[i] = 0;

	return tree;
}

int main()
{
	first_use(128);
	auto tree = bplus_tree_init();
	//bplus_tree_dump(tree);
	//system("pause"); return 0;
	printf_s("cool\n");
	auto beg = clock();
	for (auto i = 1; i < 60; ++i) {
		bplus_tree_insert(tree, i, rel_ptr<int>::null());
	}
	bplus_tree_dump(tree);
	system("pause");
	printf_s("write: %lf\nmem: %lf\nalloc: %lf\n",
		double(clock() - beg) / CLOCKS_PER_SEC,
		(double)alo / CLOCKS_PER_SEC);

	beg = clock();
	for (auto i = 1; i < 60; ++i) {
		bplus_tree_get(tree, i);
	}
	printf_s("read: %lf\n",
		double(clock() - beg) / CLOCKS_PER_SEC);
	//bplus_tree_dump(tree);
	for (auto i = 5; i < 60; i += 5) {
		if (i == 15) {
			printf_s("fu");
		}
		bplus_tree_delete(tree, i);
	}
	bplus_tree_dump(tree);

	system("pause");
	return 0;
}
/*
void bplus_tree_deinit(struct bplus_tree *tree)
{
	int fd = open(tree->filename, O_CREAT | O_RDWR, 0644);
	assert(fd >= 0);
	assert(offset_store(fd, tree->root) == ADDR_STR_WIDTH);
	assert(offset_store(fd, _block_size) == ADDR_STR_WIDTH);
	assert(offset_store(fd, tree->file_size) == ADDR_STR_WIDTH);

	// store free blocks in files for future reuse
	struct list_head *pos, *n;
	list_for_each_safe(pos, n, &tree->free_blocks) {
		list_del(pos);
		struct free_block *block = list_entry(pos, struct free_block, link);
		assert(offset_store(fd, block->offset) == ADDR_STR_WIDTH);
		free(block);
	}

	bplus_close(tree->fd);
	free(tree->caches);
	free(tree);
}
*/
#define _BPLUS_TREE_DEBUG
#ifdef _BPLUS_TREE_DEBUG

#define MAX_LEVEL 10

struct node_backlog {
	/* Node backlogged */
	rel_ptr<bplus_node> offset;
	/* The index next to the backtrack point, must be >= 1 */
	int next_sub_idx;
};

static inline int children(rel_ptr<bplus_node> node)
{
	assert(!is_leaf(node));
	return node->children;
}

static void node_key_dump(rel_ptr<bplus_node> node)
{
	int i;
	if (is_leaf(node)) {
		printf("leaf:");
		for (i = 0; i < node->children; i++) {
			printf(" %d", key(node)[i]);
		}
	}
	else {
		printf("node:");
		for (i = 0; i < node->children - 1; i++) {
			printf(" %d", key(node)[i]);
		}
	}
	printf("\n");
}

static void draw(struct bplus_tree *tree, rel_ptr<bplus_node> node, struct node_backlog *stack, int level)
{
	int i;
	for (i = 1; i < level; i++) {
		if (i == level - 1) {
			printf("%-8s", "+-------");
		}
		else {
			if (!stack[i - 1].offset.is_null()) {
				printf("%-8s", "|");
			}
			else {
				printf("%-8s", " ");
			}
		}
	}
	node_key_dump(node);
}

void bplus_tree_dump(struct bplus_tree *tree)
{
	int level = 0;
	rel_ptr<bplus_node> node = tree->root;
	struct node_backlog *p_nbl = NULL;
	struct node_backlog nbl_stack[MAX_LEVEL];
	struct node_backlog *top = nbl_stack;

	for (; ;) {
		if (!node.is_null()) {
			/* non-zero needs backward and zero does not */
			int sub_idx = p_nbl != NULL ? p_nbl->next_sub_idx : 0;
			/* Reset each loop */
			p_nbl = NULL;

			/* Backlog the node */
			if (is_leaf(node) || sub_idx + 1 >= children(node)) {
				top->offset.set_null();
				top->next_sub_idx = 0;
			}
			else {
				top->offset = node->self;
				top->next_sub_idx = sub_idx + 1;
			}
			top++;
			level++;

			/* Draw the node when first passed through */
			if (sub_idx == 0) {
				draw(tree, node, nbl_stack, level);
			}

			/* Move deep down */
			if (is_leaf(node))
				node.set_null();
			else
				node = sub(node)[sub_idx];
		}
		else {
			p_nbl = top == nbl_stack ? NULL : --top;
			if (p_nbl == NULL) {
				/* End of traversal */
				break;
			}
			node = p_nbl->offset;
			level--;
		}
	}
}

#endif