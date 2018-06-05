#ifndef UTILS_H
#define UTILS_H
#include <assert.h>
#include <stdint.h>
#include <libpmemobj.h>

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

struct bz_memory_pool {
	PMEMmutex mem_lock;
	PMEMobjpool * pop_;
	void init(PMEMobjpool *pop, PMEMoid base_oid) {
		pop_ = pop;
		rel_ptr<uint64_t>::set_base(base_oid);
		rel_ptr<rel_ptr<uint64_t>>::set_base(base_oid);
	}
	
#ifndef IS_PMEM

	POBJ_LIST_HEAD(bz_node_list, struct bz_node_block) head_;
	void prev_alloc() {
		uint32_t node_sz = NODE_ALLOC_SIZE;
		for (size_t i = 0; i < PRE_ALLOC_NUM; ++i) {
			POBJ_LIST_INSERT_NEW_TAIL(pop_, &head_, entry,
				sizeof(bz_node_block) + NODE_ALLOC_SIZE,
				NULL, NULL);
		}
	}
	void acquire(rel_ptr<rel_ptr<uint64_t>> ptr) {
		pmemobj_mutex_lock(pop_, &mem_lock);
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
		pmemobj_mutex_unlock(pop_, &mem_lock);
	}
	void release(rel_ptr<rel_ptr<uint64_t>> ptr) {
		pmemobj_mutex_lock(pop_, &mem_lock);
		PMEMoid oid = ptr->oid();
		//right?
		oid.off -= sizeof(bz_node_block);
		TOID(struct bz_node_block) back = TOID(struct bz_node_block)(oid);
		
		TX_BEGIN(pop_) {
			pmemobj_tx_add_range_direct(ptr.abs(), sizeof(uint64_t));
			POBJ_LIST_INSERT_TAIL(pop_, &head_, back, entry);
			*ptr = rel_ptr<uint64_t>();
		} TX_END;
		pmemobj_mutex_unlock(pop_, &mem_lock);
	}

#else
	
	uint32_t front_, back_;
	uint64_t nodes[MAX_ALLOC_NUM];
	void prev_alloc() {
		front_ = back_ = 0;
		for (int i = 0; i < PRE_ALLOC_NUM; ++i) {
			PMEMoid oid;
			pmemobj_alloc(pop_, &oid, NODE_ALLOC_SIZE, TOID_TYPE_NUM(struct bz_node_block), NULL, NULL);
			assert(!OID_IS_NULL(oid));
			nodes[back_] = rel_ptr<uint64_t>(oid).rel();
			pmem_persist(&nodes[back_], sizeof(uint64_t));
			++back_;
		}
		pmem_persist(&front_, sizeof(uint32_t));
		pmem_persist(&back_, sizeof(uint32_t));
	}
	void acquire(rel_ptr<rel_ptr<uint64_t>> ptr) {
		pmemobj_mutex_lock(pop_, &mem_lock);
		if (front_ == back_) {
			PMEMoid oid;
			pmemobj_alloc(pop_, &oid, NODE_ALLOC_SIZE, TOID_TYPE_NUM(struct bz_node_block), NULL, NULL);
			assert(!OID_IS_NULL(oid));
			*ptr = rel_ptr<uint64_t>(oid);
		}
		else {
			*ptr = rel_ptr<uint64_t>(nodes[front_]);
			front_ = (front_ + 1) % MAX_ALLOC_NUM;
			pmem_persist(&front_, sizeof(uint32_t));
		}
		pmem_persist(ptr.abs(), sizeof(uint64_t));
		pmemobj_mutex_unlock(pop_, &mem_lock);
	}
	void release(rel_ptr<rel_ptr<uint64_t>> ptr) {
		if (ptr->is_null())
			return;
		pmemobj_mutex_lock(pop_, &mem_lock);
		nodes[back_] = ptr->rel();
		*ptr = rel_ptr<uint64_t>();
		back_ = (back_ + 1) % MAX_ALLOC_NUM;
		assert(back_ != front_);
		pmem_persist(ptr.abs(), sizeof(uint64_t));
		pmem_persist(&back_, sizeof(uint32_t));
		pmemobj_mutex_unlock(pop_, &mem_lock);
	}

#endif // !IS_PMEM

};

#endif // !UTILS_H
