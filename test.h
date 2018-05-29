#ifndef TEST_H
#define TEST_H
#include <assert.h>
#include <iostream>
#include<iomanip>
#include <thread>
#include <vector>
#include <string.h>
#include <unordered_map>
#include <atomic>
#include "bztree.h"
#include "bzerrno.h"
using namespace std;

template<typename T>
struct bz_test {
	atomic<bool> flag = false;
	struct pmem_layout
	{
		bz_tree<T, rel_ptr<T>> tree;
		T data[64 * 8];
	};

	void print_log(const char * action, T * k, int ret = -1, bool pr = false) {
		if (!pr)
			return;
		while (flag.exchange(true)) {
			continue;
		}
		if (typeid(T) == typeid(char)) {
			cout << "[" << this_thread::get_id() << "] ";
			cout << action << " key " << k;
			if (ret >= 0)
				cout << " : " << ret;
			cout << endl;
		}
		else {
			cout << "[" << this_thread::get_id() << "] ";
			cout << action << " key " << *k;
			if (ret >= 0)
				cout << " : " << ret;
			cout << endl;
		}
		flag.store(false);
	}
	void pmem_worker(pmem_layout * top_obj, T * k, rel_ptr<T> *v,
		bool write, bool dele, bool update, bool read, bool upsert, 
		bool consolidate, bool split) 
	{
		uint32_t key_sz = typeid(T) == typeid(char) ? (uint32_t)strlen((char*)k) + 1 : sizeof(T);
		rel_ptr < bz_node<T, rel_ptr<T>>> root(top_obj->tree.root_);
		if (write) {
			print_log("INSERT", k);
			int ret = root->insert(&top_obj->tree, k, v, key_sz, key_sz + 8, 0x1);
			if (ret == EALLOCSIZE) {
				uint64_t status_rd = pmwcas_read(&root->status_);
				uint32_t rec_cnt = get_record_count(status_rd);
				print_log("EALLOCSIZE", k, rec_cnt);
			}
			else {
				assert(!ret || ret == EUNIKEY);
			}
			print_log("INSERT", k, ret);
		}
		if (dele) {
			print_log("DELETE", k);
			int ret = root->remove(&top_obj->tree, k);
			assert(!ret || ret == ENOTFOUND);
			print_log("DELETE", k, ret);
		}
		if (update) {
			print_log("UPDATE", k);
			int ret = root->update(&top_obj->tree, k, v + 1, key_sz, key_sz + 8, 0x1);
			print_log("UPDATE", k, ret);
		}
		if (upsert) {
			print_log("UPSERT", k);
			int ret = root->upsert(&top_obj->tree, k, v + 1, key_sz, key_sz + 8, 0x1);
			print_log("UPSERT", k, ret);
		}
		if (read) {
			rel_ptr<T> data;
			int ret = root->read(&top_obj->tree, k, &data, 8);
			assert(ret == ENOTFOUND || !ret);
		}
		if (consolidate) {
			int ret = root->consolidate<rel_ptr<T>>(&top_obj->tree,
				rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
			assert(!ret);

			//失败的consolidate
			rel_ptr<bz_node<T, uint64_t>> root_2(top_obj->tree.root_);
			ret = root_2->consolidate<rel_ptr<T>>(&top_obj->tree,
				rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>(0xabcd));
			assert(ret);
		}
		if (split) {
			top_obj->tree.print_tree();

			//分裂root
			rel_ptr<bz_node<T, rel_ptr<T>>> root_1(top_obj->tree.root_);
			int ret = root_1->split<rel_ptr<T>>(&top_obj->tree, rel_ptr<bz_node<T, uint64_t>>::null(),
				rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
			assert(!ret);
			top_obj->tree.print_tree();

			//分裂left
			rel_ptr<bz_node<T, uint64_t>> root_2(top_obj->tree.root_);
			uint64_t * meta_arr = root_2->rec_meta_arr();
			rel_ptr<bz_node<T, rel_ptr<T>>> left(*root_2->get_value(meta_arr[0]));
			ret = left->split<rel_ptr<T>>(&top_obj->tree, root_2,
				rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
			assert(!ret);
			top_obj->tree.print_tree();

			//失败的分裂right
			rel_ptr<bz_node<T, uint64_t>> root_3(top_obj->tree.root_);
			meta_arr = root_2->rec_meta_arr();
			rel_ptr<bz_node<T, rel_ptr<T>>> right(*root_3->get_value(meta_arr[1]));
			ret = right->split<rel_ptr<T>>(&top_obj->tree, root_3,
				rel_ptr<uint64_t>(0xabcd), rel_ptr<uint64_t>(0xabcd));
			assert(ret);
			top_obj->tree.print_tree();

			//分裂root
			rel_ptr<bz_node<T, uint64_t>> root_5(top_obj->tree.root_);
			ret = root_5->split<rel_ptr<T>>(&top_obj->tree, 
				rel_ptr<bz_node<T, uint64_t>>::null(),
				rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
			assert(!ret);
			top_obj->tree.print_tree();

			//分裂left left
			rel_ptr<bz_node<T, uint64_t>> root_6(top_obj->tree.root_);
			meta_arr = root_6->rec_meta_arr();
			rel_ptr<uint64_t> grandpa_ptr = root_6->get_value(meta_arr[0]);
			rel_ptr<bz_node<T, uint64_t>> new_left_plus(*grandpa_ptr);
			rel_ptr<bz_node<T, rel_ptr<T>>> left_left(*new_left_plus->get_value(meta_arr[0]));
			ret = left_left->split<rel_ptr<T>>(&top_obj->tree, new_left_plus,
				&root_6->status_, grandpa_ptr);
			assert(!ret);
			top_obj->tree.print_tree();
		}
	}
	void show_mem(pmem_layout * top_obj, int sz)
	{
		rel_ptr<bz_node<T, rel_ptr<T>>> root(top_obj->tree.root_);
		uint64_t * meta_arr = root->rec_meta_arr();
		int rec_cnt = get_record_count(root->status_);
		cout << setfill('0') << setw(16) << hex << root->status_ << endl;
		cout << setfill('0') << setw(16) << hex << root->length_ << endl;
		if (typeid(T) == typeid(char)) {
			unordered_map<string, int> s;
			for (int i = 0; i < rec_cnt; ++i) {
				cout << setfill('0') << setw(16) << hex << meta_arr[i];
				if (is_visiable(meta_arr[i])) {
					cout << " : " << (char*)root->get_key(meta_arr[i])
						<< ", " << (char*)(*root->get_value(meta_arr[i])).abs() << endl;
					string ke = (char*)root->get_key(meta_arr[i]);
					if (s.find(ke) != s.end())
						assert(0);
					s[ke] = 0;
				}
				else
					cout << " : NO RECORD" << endl;
			}
		}
		else {
			unordered_map<T, int> s;
			for (int i = 0; i < rec_cnt; ++i) {
				cout << setfill('0') << setw(16) << hex << meta_arr[i];
				if (is_visiable(meta_arr[i])) {
					auto k = *root->get_key(meta_arr[i]);
					auto v = **root->get_value(meta_arr[i]);
					cout << dec << " : " << k
						<< ", " << v << endl;
					T ke = *root->get_key(meta_arr[i]);
					if (s.find(ke) != s.end())
						assert(1);
					s[ke] = 0;
				}
				else
					cout << " : NO RECORD" << endl;
			}
		}
	}
	void range_scan(pmem_layout * top_obj, T * beg, T * end)
	{
		rel_ptr < bz_node<T, rel_ptr<T>>> root(top_obj->tree.root_);
		auto res = root->range_scan(beg, end);
		for (auto kv : res) {
			if (typeid(T) == typeid(char))
				cout << kv.first.get() << " : " << (char*)(*kv.second.get()).abs() << endl;
			else
				cout << dec << *kv.first << " : " << **kv.second << endl;
		}
	}
	void run(
		bool first = true,
		bool write = true,
		bool dele = false,
		bool update = false,
		bool read = false,
		bool upsert = false,
		bool consolidate = false,
		bool split = false,
		int sz = 32,
		int concurrent = 16,
		bool recovery = true) 
	{
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
			for (int i = 0; i < 64; ++i)
				if (typeid(T) == typeid(char))
					_itoa(10 * i, (char*)top_obj->data + i * 8, 10);
				else
					top_obj->data[i] = 10 * i;
		}
		tree.init(pop, top_oid);
		if (recovery) {
			tree.recovery();
		}
		if (first) {
			int ret = tree.new_root();
			rel_ptr < bz_node<T, rel_ptr<T>>> root(top_obj->tree.root_);
			assert(!ret && !root.is_null() && NODE_ALLOC_SIZE == get_node_size(root->length_));
		}

		char *char_keys[64];
		T keys[64];
		rel_ptr<T> vals[64];
		for (int i = 0; i < 64; ++i) {
			char_keys[i] = new char[10];
			_itoa(i, char_keys[i], 10);
			keys[i] = i;
			vals[i] = typeid(T) == typeid(char) ? top_obj->data + 8 * i : top_obj->data + i;
		}
		thread t[64 * 8];
		for (int i = 0; i < sz; ++i) 
			for (int j = 0; j < concurrent; ++j)
			{
				int index = i + j > sz - 1 ? sz - 1 : i + j;
				t[i * concurrent + j] = thread(&bz_test::pmem_worker,
					this, top_obj, 
					typeid(T) == typeid(char) ? (T*)char_keys[index] : &keys[index], 
					&vals[index],
					write, dele, update, read, upsert, 
					consolidate, split);
			}
		for (int i = 0; i < sz * concurrent; ++i) {
			t[i].join();
		}

		//打印
		if (!split) {
			rel_ptr<bz_node<T, uint64_t>> root(top_obj->tree.root_);
			uint64_t * meta_arr = root->rec_meta_arr();
			int rec_cnt = get_record_count(root->status_);
			show_mem(top_obj, rec_cnt);
			if (typeid(T) == typeid(char))
				range_scan(top_obj, (T*)char_keys[0], (T*)char_keys[9]);
			else
				range_scan(top_obj, &keys[0], &keys[63]);
		}

		//收工
		for (int i = 0; i < 64; ++i) {
			delete[] char_keys[i];
		}
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

	void pmwcas_worker_func(int *x, int i, int last, mdesc_pool_t pool) {
		bool done = false;
		do
		{
			auto mdesc = pmwcas_alloc(pool, 0, 0);
			if (mdesc.is_null()) {
				this_thread::sleep_for(chrono::milliseconds(1));
				continue;
			}
			pmwcas_add(mdesc, (uint64_t*)x, i, i + 1, 0);
			done = pmwcas_commit(mdesc);
			pmwcas_free(mdesc);
			this_thread::sleep_for(chrono::milliseconds(1));
		} while (!done && *x != last);
	}
	void run(int sz = 24, int concurrent = 8)
	{
		const char * fname = "test.pool";
		remove(fname);
		auto pop = pmemobj_createU(fname, "layout", PMEMOBJ_MIN_POOL, 0666);
		auto top_oid = pmemobj_root(pop, sizeof(pmwcas_layout));
		auto top_obj = (pmwcas_layout *)pmemobj_direct(top_oid);
		pmwcas_first_use(&top_obj->pool);
		pmwcas_init(&top_obj->pool, top_oid);
		
		int test_num = sz * concurrent;

		vector<thread> ts(test_num);

		for (int i = 0; i < sz; ++i)
			for (int j = 0; j < concurrent; ++j) 
			{
				ts[i * concurrent + j] = thread(&pmwcas_test::pmwcas_worker_func, this,
					top_obj->x, i, sz, &top_obj->pool);
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
