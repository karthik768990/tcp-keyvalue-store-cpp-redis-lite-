#pragma once 
#include"avl.h"
#include "hashtable.h"

struct ZSet{
    AVLNode *root = NULL;   //this is used to index by score name 
    HMap hmap;      //this is the index by name
};

struct ZNode{
    AVLNode tree;
    HNode hmap;
    double score =0;
    size_t len  = 0;
    char name[0];       //this is the flexible array so o need to fix the length before hand
};


bool   zset_insert(ZSet *zset, const char *name, size_t len, double score);
ZNode *zset_lookup(ZSet *zset, const char *name, size_t len);
void   zset_delete(ZSet *zset, ZNode *node);
ZNode *zset_seekge(ZSet *zset, double score, const char *name, size_t len);
void   zset_clear(ZSet *zset);
ZNode *znode_offset(ZNode *node, int64_t offset);

