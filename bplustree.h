/*
* Copyright (C) 2017, Leo Ma <begeekmyfriend@gmail.com>
*/

#ifndef _BPLUS_TREE_H
#define _BPLUS_TREE_H

#include "rel_ptr.h"

/* 5 node caches are needed at least for self, left and right sibling, sibling
* of sibling, parent and node seeking */
#define MIN_CACHE_NUM 5

#define list_entry(ptr, type, member) \
        ((type *)((char *)(ptr) - (size_t)(&((type *)0)->member)))

#define list_first_entry(ptr, type, member) \
	list_entry((ptr)->next, type, member)

#define list_last_entry(ptr, type, member) \
	list_entry((ptr)->prev, type, member)

#define list_for_each(pos, head) \
        for (pos = (head)->next; pos != (head); pos = pos->next)

#define list_for_each_safe(pos, n, head) \
        for (pos = (head)->next, n = pos->next; pos != (head); \
                pos = n, n = pos->next)

typedef int key_t;

struct list_head {
	struct list_head *prev, *next;
};

static inline void list_init(struct list_head *link)
{
	link->prev = link;
	link->next = link;
}

static inline void
__list_add(struct list_head *link, struct list_head *prev, struct list_head *next)
{
	link->next = next;
	link->prev = prev;
	next->prev = link;
	prev->next = link;
}

static inline void __list_del(struct list_head *prev, struct list_head *next)
{
	prev->next = next;
	next->prev = prev;
}

static inline void list_add(struct list_head *link, struct list_head *prev)
{
	__list_add(link, prev, prev->next);
}

static inline void list_add_tail(struct list_head *link, struct list_head *head)
{
	__list_add(link, head->prev, head);
}

static inline void list_del(struct list_head *link)
{
	__list_del(link->prev, link->next);
	list_init(link);
}

static inline int list_empty(const struct list_head *head)
{
	return head->next == head;
}

typedef struct bplus_node {
	rel_ptr<bplus_node> parent;
	rel_ptr<bplus_node> prev;
	rel_ptr<bplus_node> next;
	int type;
	/* If leaf node, it specifies  count of entries,
	* if non-leaf node, it specifies count of children(branches) */
	int children;
	bplus_node(int t) : type(t), children(0)
	{
		parent.set_null();
		prev.set_null();
		next.set_null();
	}
} bplus_node;

/*
struct bplus_non_leaf {
rel_ptr<bplus_node> self;
rel_ptr<bplus_node> parent;
rel_ptr<bplus_node> prev;
rel_ptr<bplus_node> next;
int type;
int children;
key_t key[BPLUS_MAX_ORDER - 1];
rel_ptr<bplus_node> sub_ptr[BPLUS_MAX_ORDER];
};
struct bplus_leaf {
rel_ptr<bplus_node> self;
rel_ptr<bplus_node> parent;
rel_ptr<bplus_node> prev;
rel_ptr<bplus_node> next;
int type;
int entries;
key_t key[BPLUS_MAX_ENTRIES];
long data[BPLUS_MAX_ENTRIES];
};
*/

typedef struct free_block {
	struct list_head link;
	rel_ptr<bplus_node> offset;
} free_block;

struct bplus_tree {
	PMEMobjpool * pop;
	uint64_t block_size;
	uint64_t level;
	rel_ptr<bplus_node> root;
};

void bplus_tree_dump(struct bplus_tree *tree);
rel_ptr<int> bplus_tree_get(struct bplus_tree *tree, key_t key);
int bplus_tree_put(struct bplus_tree *tree, key_t key, rel_ptr<int> data);
rel_ptr<int> bplus_tree_get_range(struct bplus_tree *tree, key_t key1, key_t key2);
struct bplus_tree *bplus_tree_init(char *filename, int block_size);
void bplus_tree_deinit(struct bplus_tree *tree);
int bplus_open(char *filename);
void bplus_close(int fd);

#endif  /* _BPLUS_TREE_H */