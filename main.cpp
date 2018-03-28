#include <iostream>
#include <stdio.h>
#include <assert.h>
#include "PMwCAS.h"


using namespace std;

gc_t *	gc;
PMEMobjpool * pop;


void construct(mdesc_pool_t pool, off_t off) 
{
	//Obj ooo(6767);

	gc_register(gc);

	/*
	* Some processing which references the object(s).
	* The readers must indicate the critical path where
	* they actively reference any objects.
	*/
	gc_crit_enter(gc);
	
	//pmwcas_free(mdesc, gc);
END:
	gc_crit_exit(gc);
	gc_cycle(gc);

}


int _main() {
	
	const char * pool_file = "pool.pool";
	const char * layout = "layout_id";
	
	

	system("pause");
	return 0;
}

void writer()
{
	/*
	* Remove the object from the lock-free container.  The
	* object is no longer globally visible.  Not it can be
	* staged for destruction -- add it to the limbo list.
	*/
	//obj = lockfree_remove(container, key);
	//gc_limbo(gc, (void *)&obj);

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