#include <iostream>
#include <stdio.h>
#include <assert.h>
#include "PMwCAS.h"

using namespace std;

typedef struct {
	int val = 3;
	gc_entry_t	gc_entry;
} obj_t;

static gc_t *	gc;
PMEMobjpool * pop;

void sys_init(mdesc_pool_t pool)
{
	gc = gc_create(offsetof(obj_t, gc_entry), reclaim_PMwCAS, gc);
	recovery_PMwCAS(pool);
	assert(gc != NULL);
}

void reader(mdesc_pool_t pool, uint64_t * addr) 
{
	gc_register(gc);

	/*
	* Some processing which references the object(s).
	* The readers must indicate the critical path where
	* they actively reference any objects.
	*/
	gc_crit_enter(gc);
	mdesc_t mdesc = alloc_PMwCAS(pool, 0);
	mdesc_t mdesc2 = alloc_PMwCAS(pool, 0);

	if (mdesc == nullptr)
	{
		cout << "err alloc_PMwCAS" << endl;
		goto END;
	}
	for (off_t i = 2; i >= 0; --i)
		if (!add_entry(pop, mdesc, addr + i, 0, i * 10 + 1, 0))
			cout << "err add_entry " << i << endl;

	for (off_t i = 0; i <= 2; ++i)
		if (!add_entry(pop, mdesc, addr + i, i * 10 + 1, i * 10 + 2, 0))
			cout << "err add_entry " << i << endl;

	if (mdesc2 == nullptr)
	{
		cout << "err alloc_PMwCAS" << endl;
		goto END;
	}
	for (off_t i = 1; i < 4; ++i)
		if (!add_entry(pop, mdesc2, addr + i, i * 10 + 1, i * 10 + 3, 0))
			cout << "err add_entry " << i << endl;

	if (PMwCAS(mdesc))
		cout << "mdesc1 success!" << endl;
	else
		cout << "mdesc1 failed!" << endl;
	if (PMwCAS(mdesc2))
		cout << "mdesc2 success!" << endl;
	else
		cout << "mdesc2 failed!" << endl;

	free_PMwCAS(mdesc, gc);
	free_PMwCAS(mdesc2, gc);
END:
	gc_crit_exit(gc);
	gc_cycle(gc);
}

void writer(obj_t *obj)
{
	/*
	* Remove the object from the lock-free container.  The
	* object is no longer globally visible.  Not it can be
	* staged for destruction -- add it to the limbo list.
	*/
	//obj = lockfree_remove(container, key);
	gc_limbo(gc, (void *)obj);

	/*
	* Checkpoint: run a G/C cycle attempting to reclaim *some*
	* objects previously added to the limbo list.  This should be
	* called periodically for incremental object reclamation.
	*
	* WARNING: All gc_cycle() calls must be serialised (using a
	* mutex or by running in a single-threaded manner).
	*/
	gc_cycle(gc);
}

struct root
{
	uint64_t nums[10];
	descriptor_pool pool;
};

void tfunc(off_t off)
{

}

int main() {
	const char * pool_file = "pool.pool";
	const char * layout = "layout_id";
	
	//remove(pool_file);
	//pop = pmemobj_createU(pool_file, layout, 8000000+ PMEMOBJ_MIN_POOL + sizeof(descriptor_pool), 0666);
	
	pop = pmemobj_openU(pool_file, layout);
	
	root* rp = (root *)pmemobj_direct(pmemobj_root(pop, sizeof(root)));
	sys_init(&rp->pool);
	//init_pool(&rp->pool);
	reader(&rp->pool, rp->nums);
	for (off_t i = 0; i < 6; ++i)
		cout << "final " << rp->nums[i] << endl;
	system("pause");
	return 0;
}
int _main() {
	const char * pool_file = "pool.pool";
	const char * layout = "layout_id";

	//remove(pool_file);
	//pop = pmemobj_createU(pool_file, layout, 8000000 + PMEMOBJ_MIN_POOL + sizeof(descriptor_pool), 0666);

	pop = pmemobj_openU(pool_file, layout);

	root* rp = (root *)pmemobj_direct(pmemobj_root(pop, sizeof(root)));
	auto x = rp->pool;
	//gc_init();
	//init_pool(&rp->pool);
	//reader(&rp->pool, rp->nums);
	for (off_t i = 0; i < 6; ++i)
		cout << "final " << rp->nums[i] << endl;
	system("pause");
	return 0;
}