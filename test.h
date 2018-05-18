#ifndef TEST_H
#define TEST_H
#include <assert.h>
#include <iostream>
#include<iomanip>
#include <thread>
#include <vector>
#include "bztree.h"
#include "bztree.cpp"
using namespace std;


struct pmem_test {
	struct pmem_layout
	{
		bz_tree<int, int> tree;
	};
	void pmem_worker(pmem_test::pmem_layout * top_obj, int *k, int *v, int extra, bool write, bool dele) {
		for (int i = 0; i < extra; ++i) {
			if (write) {
				int ret = top_obj->tree.root_->insert(&top_obj->tree, k + i, v + i, 4, 8, 1);
				assert(!ret || ret == EUNIKEY);
			}
			if (dele) {
				int ret = top_obj->tree.root_->remove(&top_obj->tree, k + i);
				assert(!ret || ret == ENOTFOUND);
			}
		}
	}
	void show_mem(pmem_layout * top_obj, int sz)
	{
		auto &root = *top_obj->tree.root_;
		uint64_t * meta_arr = root.rec_meta_arr();
		int rec_cnt = get_record_count(root.status_);
		cout << setfill('0') << setw(16) << hex << root.status_ << endl;
		cout << setfill('0') << setw(16) << hex << root.length_ << endl;
		for (int i = 0; i < rec_cnt; ++i) {
			cout << setfill('0') << setw(16) << hex << meta_arr[i];
			if (is_visiable(meta_arr[i]))
				cout << dec << " : " << *root.get_key(meta_arr[i]) 
				<< ", " << *root.get_value(meta_arr[i]) << endl;
			else
				cout << " : NO RECORD" << endl;
		}
	}
	void run(
		bool first = true, 
		bool write = true, 
		bool dele = true,
		int node_sz = 600 * 16, 
		int sz = 24
	) {
		const char * fname = "test.pool";
		PMEMobjpool * pop;
		if (first) {
			remove(fname);
			pop = pmemobj_createU(fname, "layout", PMEMOBJ_MIN_POOL * 10, 0666);
		}
		else
		{
			pop = pmemobj_openU(fname, "layout");
		}
		assert(pop);

		auto top_oid = pmemobj_root(pop, sizeof(pmem_layout));
		auto top_obj = (pmem_layout *)pmemobj_direct(top_oid);
		assert(!OID_IS_NULL(top_oid) && top_obj);
		auto &tree = top_obj->tree;
		if (first) {
			tree.first_use();
		}
		tree.init(pop, top_oid);
		if (first) {
			int ret = tree.alloc_node(&tree.root_, tree.root_, node_sz);
			assert(!ret && !tree.root_.is_null() && node_sz == get_node_size(tree.root_->length_));
		}
		else
		{
			show_mem(top_obj, 24);
		}

		int keys[512], vals[512];
		for (int i = 0; i < 512; ++i) {
			keys[i] = i;
			vals[i] = i * 10;
		}
		thread t[512];
		int extra = 8;
		for (int i = 0; i < sz; ++i) {
			t[i] = thread(&pmem_test::pmem_worker, 
				this, top_obj, &keys[i], &vals[i], 
				sz - i > extra ? extra : sz - i,
				write, dele);
		}
		for (int i = 0; i < sz; ++i) {
			t[i].join();
		}

		auto root = &*tree.root_;
		uint64_t * meta_arr = root->rec_meta_arr();
		int rec_cnt = get_record_count(root->status_);
		int blk_sz = get_block_size(root->status_);
		int del_sz = get_delete_size(root->status_);
		assert(rec_cnt * 8 == blk_sz);
		int real_cnt = 0;
		for (int i = 0; i < rec_cnt; ++i)
			if (is_visiable(meta_arr[i]))
				++real_cnt;

		show_mem(top_obj, 24);

		tree.finish();
		pmemobj_close(pop);
	}
};

struct pmwcas_test
{
	struct pmwcas_layout
	{
		pmwcas_pool pool;
		int x[1000];
	};

	void pmwcas_worker_func(int *x, int i, mdesc_pool_t pool) {
		bool done = false;
		do
		{
			auto mdesc = pmwcas_alloc(pool, 0, 0);
			if (mdesc.is_null()) {
				this_thread::sleep_for(chrono::milliseconds(5));
				continue;
			}
			pmwcas_add(mdesc, (uint64_t*)x, i, i + 1, 0);
			done = pmwcas_commit(mdesc);
			pmwcas_free(mdesc);
		} while (!done);
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
	}
	void run()
	{
		const char * fname = "test.pool";
		remove(fname);
		auto pop = pmemobj_createU(fname, "layout", PMEMOBJ_MIN_POOL, 0666);
		auto top_oid = pmemobj_root(pop, sizeof(pmwcas_layout));
		auto top_obj = (pmwcas_layout *)pmemobj_direct(top_oid);
		pmwcas_first_use(&top_obj->pool);
		pmwcas_init(&top_obj->pool, top_oid);
		
		int test_num = 512;

		vector<thread> ts(test_num);

		for (int i = 0; i < test_num; ++i) {
			ts[i] = thread(&pmwcas_test::pmwcas_worker_func, this, top_obj->x, i, &top_obj->pool);
		}
		for (int i = 0; i < test_num; ++i)
			ts[i].join();

		cout << top_obj->x[0] << endl;

		pmwcas_finish(&top_obj->pool);
		pmemobj_close(pop);
	}
};

struct gc_test
{
	void run()
	{
		gc_t * gc = gc_create(offsetof(struct pmwcas_entry, gc_entry), NULL, NULL);
		gc_register(gc);
		gc_crit_enter(gc);
		int x = 10; /* deal with shared variables */
		gc_crit_exit(gc);
		gc_limbo(gc, (void*)&x); /* add to garbage list */
		gc_cycle(gc); /* must be serialized */
		gc_full(gc, 500);
		gc_destroy(gc);
	}
};
#endif // !TEST_H
