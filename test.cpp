#include <assert.h>
#include <iostream>
#include <thread>
#include <functional>
#include "bztree.h"
using namespace std;
struct pmwcas_layout
{
	pmwcas_pool pool;
	int x[1000];
};

void worker_func(int *x, int i, mdesc_pool_t pool) {
	bool done = false;
	do
	{
		auto mdesc = pmwcas_alloc(pool, 0, 0);
		if (mdesc.is_null()) {
			throw exception("pmwcas full");
		}
		pmwcas_add(mdesc, (uint64_t*)x, i, i + 1, 0);
		done = pmwcas_commit(mdesc);
		pmwcas_free(mdesc);
	} while (!done);
	std::this_thread::sleep_for(std::chrono::milliseconds(500));
}
void test_pmwcas()
{
	
	const char * fname = "test.pool";
	remove(fname);
	auto pop = pmemobj_createU(fname, "layout", PMEMOBJ_MIN_POOL, 0666);
	auto top_oid = pmemobj_root(pop, sizeof(pmwcas_layout));
	auto top_obj = (pmwcas_layout *)pmemobj_direct(top_oid);
	pmwcas_first_use(&top_obj->pool);
	pmwcas_init(&top_obj->pool, top_oid);

	/*
	auto mdesc = pmwcas_alloc(&top_obj->pool, 0, 0);
	pmwcas_add(mdesc, (uint64_t*)top_obj->x, 0, 1, 0);
	bool done = pmwcas_commit(mdesc);
	pmwcas_free(mdesc);

	mdesc = pmwcas_alloc(&top_obj->pool, 0, 0);
	pmwcas_add(mdesc, (uint64_t*)top_obj->x, 1, 2, 0);
	done = pmwcas_commit(mdesc);
	pmwcas_free(mdesc);
	*/

	thread ts[500];

	for (int i = 0; i < 256; ++i) {
		ts[i] = thread(worker_func, top_obj->x, i, &top_obj->pool);
	}
	for (int i = 0; i < 256; ++i)
		ts[i].join();
	cout << top_obj->x[0] << endl;

	pmwcas_finish(&top_obj->pool);
	pmemobj_close(pop);
}

struct pmem_layout
{
	pmwcas_pool pool;
	bz_tree tree;
};
void test_bz_alloc()
{
	const char * fname = "test.pool";
	remove(fname);
	auto pop = pmemobj_createU(fname, "layout", PMEMOBJ_MIN_POOL, 0666);
	auto top_oid = pmemobj_root(pop, sizeof(pmem_layout));
	auto top_obj = (pmem_layout *)pmemobj_direct(top_oid);
	pmwcas_first_use(&top_obj->pool);
	pmwcas_init(&top_obj->pool, top_oid);
	bz_init(&top_obj->tree, pop, &top_obj->pool, top_oid);

	int ret = bz_alloc(&top_obj->tree, &top_obj->tree.root, 64);
	assert(!top_obj->tree.root.is_null());
	cout << "TEST BZ ALLOC OK" << endl;

	pmwcas_finish(&top_obj->pool);
	pmemobj_close(pop);
}

void test_gc()
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