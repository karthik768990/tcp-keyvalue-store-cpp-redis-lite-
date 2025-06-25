#include <assert.h>
#include <string.h>
#include <stdlib.h>

#include "zset.h"
#include "common.h"
#include "avl.h"
#include "hashtable.h"


static ZNode *znode_new(const char *name,size_t len,double score){
    ZNode *node = (ZNode *)malloc(sizeof(ZNode)+len);
    assert(node);
    avl_init(&node->tree);
    node->hmap.next = NULL;
    node->hmap.hcode = str_hash((uint8_t *)name,len);
    node->score = score;
    node->len = len;
    memcpy(&node->name[0],name,len);
    return node;
}

static void znode_del(ZNode *node){
    free(node);
}

static size_t min(size_t lhs,size_t rhs){
    return lhs<rhs ? lhs : rhs;
}

//compare the kv paris by the (score,name) tuple
static bool zless(AVLNode *lhs,double score,const char *name,size_t len){
    ZNode *zl = container_of(lhs,ZNode,tree);
    if(zl->score != score) return zl->score <score;

    int rv = memcmp(zl->name,name,min(zl->len,len));
    if(rv!= 0) return rv<0;

    return zl->len < len;
}

static bool zless(AVLNode *lhs,AVLNode *rhs){
    ZNode *zr = container_of(rhs,ZNode,tree);
    return zless(lhs,zr->score,zr->name,zr->len);
}

//insert into the AVL Treee
static void tree_insert(ZSet *zset,ZNode *znode){
    AVLNode *parent = NULL; //the supplied node will be the child of this node
    AVLNode **from = &zset->root;
    while(*from){
        parent = *from;
        from = zless(&znode->tree,parent) ? &parent->left : &parent->right;
    }
    *from = &znode->tree;
    znode->tree.parent = parent;
    zset->root = avl_fix(&znode->tree);
}

//update the score of the existing node 
static void zset_update(ZSet *zset,ZNode *znode,double score){
    if(znode->score == score) return;
    //if not then detach the tre node and then upate the value and reinsert it 
    zset->root = avl_del(&znode->tree);
    avl_init(&znode->tree);
    
    znode->score  = score;
    tree_insert(zset,znode);
}

//add a new score name tuple if it  is not possible then update the tuple if it is already existing
bool zset_insert(ZSet *zset,const char *name,size_t len,double score){
    ZNode *znode = zset_lookup(zset,name,len);
    if(znode){
        zset_update(zset,znode,score);
        return false;
    }else{
        znode = znode_new(name,len,score);
        hm_insert(&zset->hmap,&znode->hmap);
        tree_insert(zset,znode);
        return true;
    }
}

// a helper structure for the hash table look up 
struct HKey{
    HNode node;
    const char *name =  NULL;
    size_t len = 0;
};

static bool hcmp(HNode *node,HNode *key){
    ZNode *znode = container_of(node,ZNode,hmap); //this container of returns the pointer to the entire structure but not the member of the structure 
    HKey *hkey = container_of(key,HKey,node);
    if(znode->len != hkey->len) return false;

    return 0 == memcmp(znode->name,hkey->name,znode->len);
}

//now the function to look up by name 
ZNode *zset_lookup(ZSet *zset,const char *name,size_t len){
    if(!zset->root) return NULL;
    HKey key;
    key.node.hcode = str_hash((uint8_t *)name,len);
    key.name  = name;
    key.len = len;
    HNode *found = hm_lookup(&zset->hmap,&key.node,&hcmp);
    return found? container_of(found,ZNode,hmap) : NULL;
}

//to deete a node 
void zset_delete(ZSet *zset,ZNode *znode){
    //first remove the key from the hash table
    HKey key;
    key.node.hcode = znode->hmap.hcode;
    key.name = znode->name;
    key.len = znode->len;
    HNode *found = hm_delete(&zset->hmap,&key.node,&hcmp);
    assert(found);
    //remove itfrom teh tree
    zset->root = avl_del(&znode->tree);
    //now deallocating the space for teh ndoe
    znode_del(znode);
}

//find teh first score,name tuple that is > = key
ZNode *zset_seekge(ZSet *zset,double score,const char *name,size_t len){
    AVLNode *found = NULL;
    for(AVLNode *node = zset->root;node;){
        if(zless(node,score,name,len)) node = node->right;
        else{
            found = node;
            node = node->left;
        }
    }
    return found ? container_of(found,ZNode,tree) : NULL;
}

//offset into the suceedign or preceeding nod e
ZNode *znode_offset(ZNode *node,int64_t offset){
    AVLNode *tnode = node ? avl_offset(&node->tree,offset) : NULL;
    return tnode ? container_of(tnode,ZNode,tree) : NULL;
}
static void tree_dispose(AVLNode *node){
    if(!node) return;
    tree_dispose(node->left);
    tree_dispose(node->right);
    znode_del(container_of(node,ZNode,tree));
}

//now destroy the entire zset
void zset_clear(ZSet *zset){
    hm_clear(&zset->hmap);
    tree_dispose(zset->root);
    zset->root = NULL;
}

