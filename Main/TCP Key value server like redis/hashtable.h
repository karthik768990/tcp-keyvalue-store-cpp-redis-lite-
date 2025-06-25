#pragma once


#include <stddef.h>
#include<stdint.h>

//hastable node
struct HNode{
    HNode *next = NULL;
    uint64_t hcode = 0;
};

// a simple fixed size hashtable and it uses 2 hashtable for the look up 
struct HTab{
    HNode **tab = NULL;
    size_t mask = 0;
    size_t size = 0;

};

//the real hash map which consists of two hash trab;e for the look up and can do the prograssive rehahsing work 
struct HMap{
    HTab newer;
    HTab older;
    size_t migrate_pos = 0;
};


HNode *hm_lookup(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *));
void   hm_insert(HMap *hmap, HNode *node);
HNode *hm_delete(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *));
void   hm_clear(HMap *hmap);
size_t hm_size(HMap *hmap);
// invoke the callback on each node until it returns false
void   hm_foreach(HMap *hmap, bool (*f)(HNode *, void *), void *arg);
