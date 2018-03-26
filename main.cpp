#include <iostream>
#include <stdio.h>
#include <assert.h>
#include "PMwCAS.h"


using namespace std;


struct Obj {
	uint64_t val;
	rel_ptr<Obj> left, right;
	Obj(uint64_t x) : val(x) {};
};

UCHAR* rel_ptr<Obj>::base_address;

struct root
{
	pmwcas_pool pool;
	Obj objs[100];
};

gc_t *	gc;
PMEMobjpool * pop;

void first_use(root * rp)
{

	pmwcas_first_use(&rp->pool);
	for (off_t i = 0; i < 100; ++i) {
		rp->objs[i].val = i + 1;
		rp->objs[i].left.set_null();
		rp->objs[i].right.set_null();
		persist(rp->objs + i, 8);
	}
}

void sys_init(root* rp)
{
	rel_ptr<Obj>::set_base((UCHAR*)rp);
	pmwcas_init((UCHAR*)rp);
	pmwcas_recovery(&rp->pool);
	gc = gc_create(offsetof(pmwcas_entry, gc_entry), pmwcas_reclaim, gc);
	assert(gc != NULL);
}

int constr(PMEMobjpool *pop, void* ptr, void *arg)
{
	Obj * p = (Obj*)ptr;
	p->val = (uint64_t)arg;
	p->left.set_null();
	p->right.set_null();
	pmemobj_persist(pop, p, sizeof(Obj));
	return 0;
}

void construct(mdesc_pool_t pool, root * rp, off_t off) 
{
	Obj ooo(6767);

	gc_register(gc);

	/*
	* Some processing which references the object(s).
	* The readers must indicate the critical path where
	* they actively reference any objects.
	*/
	gc_crit_enter(gc);
	mdesc_t mdesc = pmwcas_alloc(pool, 0);
	rel_ptr<Obj>* poo;
	if (mdesc.is_null())
	{
		cout << "err alloc_PMwCAS" << endl;
		
	}
	if (poo = pmwcas_reserve<Obj>(mdesc, 
		(uint64_t*)&rp->objs[off].left, 
		rp->objs[off].left,
		0ULL))
		cout << "add success!" << endl;
	else
		cout << "add failed !" << endl;

	auto tmp = rel_ptr<Obj>(rp->objs + 34);
	
	*poo = tmp;
	
	if (pmwcas_commit(mdesc)) {
		cout << "mdesc " << off << " success!" << endl;
	}
	else
		cout << "mdesc "<<off<<" failed!" << endl;

	pmwcas_free(mdesc, gc);
END:
	gc_crit_exit(gc);
	gc_cycle(gc);
	cout << "left " << rp->objs[0].left->val << endl;

}


int main() {
	
	const char * pool_file = "pool.pool";
	const char * layout = "layout_id";
	
	//remove(pool_file);
	//pop = pmemobj_createU(pool_file, layout, 8000000+ PMEMOBJ_MIN_POOL, 0666);
	
	pop = pmemobj_openU(pool_file, layout);
	
	root* rp = (root *)pmemobj_direct(pmemobj_root(pop, sizeof(root)));
	//first_use(rp);
	sys_init(rp);
	construct(&rp->pool, rp, 0);

	system("pause");
	return 0;
}

void writer()
{
	Obj obj(2);
	/*
	* Remove the object from the lock-free container.  The
	* object is no longer globally visible.  Not it can be
	* staged for destruction -- add it to the limbo list.
	*/
	//obj = lockfree_remove(container, key);
	gc_limbo(gc, (void *)&obj);

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