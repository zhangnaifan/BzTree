#ifndef UTILS_H
#define UTILS_H
#include <assert.h>
#include <stdint.h>
#include "PMwCAS.h"
#define MAX_PATH_DEPTH	16

struct bz_path_stack
{
	uint64_t nodes[MAX_PATH_DEPTH];
	int child_ids[MAX_PATH_DEPTH];
	int count = 0;
	void push(uint64_t node, int child_id) {
		assert(count < MAX_PATH_DEPTH);
		nodes[count] = node;
		child_ids[count] = child_id;
		++count;
	}
	void push() {
		++count;
	}
	void pop() {
		assert(count > 0);
		--count;
	}
	void reset() {
		count = 0;
	}
	bool empty() {
		return count <= 0;
	}
	uint64_t get_node() {
		assert(count > 0);
		return nodes[count - 1];
	}
	int get_child_id() {
		assert(count > 0);
		return child_ids[count - 1];
	}
};

POBJ_LAYOUT_BEGIN(layout_name);
POBJ_LAYOUT_TOID(layout_name, struct bz_node_block);
POBJ_LAYOUT_END(layout_name);

struct bz_node_block {
	POBJ_LIST_ENTRY(struct bz_node_block) entry;
};

int node_construct(PMEMobjpool *pop, void *ptr, void *arg)
{
	uint32_t node_sz = *(uint32_t*)arg;
	rel_ptr<bz_node<uint64_t, uint64_t>> node((bz_node<uint64_t, uint64_t>*)((char*)ptr + sizeof(bz_node_block)));
	set_node_size(node->length_, node_sz);
	return 0;
}

struct bz_memory_pool {
	POBJ_LIST_HEAD(bz_node_list, struct bz_node_block) head_;
	PMEMmutex lock;
	PMEMobjpool * pop_;

	void init(PMEMobjpool *pop) {
		pop_ = pop;
	}
	void prev_alloc(size_t num_blks) {
		uint32_t node_sz = NODE_ALLOC_SIZE;
		for (size_t i = 0; i < num_blks; ++i) {
			POBJ_LIST_INSERT_NEW_TAIL(pop_, &head_, entry,
				sizeof(bz_node_block) + NODE_ALLOC_SIZE,
				node_construct, (void*)&node_sz);
		}
	}
	void acquire(rel_ptr<rel_ptr<uint64_t>> ptr) {
		pmemobj_mutex_lock(pop_, &lock);
		TOID(struct bz_node_block) front = POBJ_LIST_FIRST(&head_);
		if (!TOID_IS_NULL(front)) {
			TX_BEGIN(pop_) {
				pmemobj_tx_add_range_direct(ptr.abs(), sizeof(uint64_t));
				*ptr = (uint64_t*)((char*)pmemobj_direct(front.oid) + sizeof(bz_node_block));
				POBJ_LIST_REMOVE(pop_, &head_, front, entry);
			} TX_END;
		}
		else {
			TX_BEGIN(pop_) {
				pmemobj_tx_add_range_direct(ptr.abs(), sizeof(uint64_t));
				PMEMoid oid = pmemobj_tx_alloc(sizeof(bz_node_block) + NODE_ALLOC_SIZE, TOID_TYPE_NUM(struct bz_node_block));
				*ptr = (uint64_t*)((char*)pmemobj_direct(oid) + sizeof(bz_node_block));
			} TX_END;
		}
		pmemobj_mutex_unlock(pop_, &lock);
	}
	void release(rel_ptr<rel_ptr<uint64_t>> ptr) {
		pmemobj_mutex_lock(pop_, &lock);
		PMEMoid oid = ptr->oid();
		//right?
		oid.off -= sizeof(bz_node_block);
		TOID(struct bz_node_block) back = TOID(struct bz_node_block)(oid);
		
		TX_BEGIN(pop_) {
			pmemobj_tx_add_range_direct(ptr.abs(), sizeof(uint64_t));
			POBJ_LIST_INSERT_TAIL(pop_, &head_, back, entry);
			*ptr = rel_ptr<uint64_t>();
		} TX_END;
		pmemobj_mutex_unlock(pop_, &lock);
	}
};

#endif // !UTILS_H
