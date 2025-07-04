#include <assert.h>
#include "avl.h"

static uint32_t max(uint32_t lhs,uint32_t rhs){
    return rhs<lhs ? lhs :  rhs;
}

//maintain the height and the cnt field
static void avl_update(AVLNode *node){
    node->height = 1+ max(avl_height(node->left),avl_height(node->right));
    node->cnt = 1+ avl_cnt(node->left)+avl_cnt(node->right);
}

static AVLNode *rot_left(AVLNode *node){
    AVLNode *parent = node->parent;
    AVLNode *new_node = node->right;
    AVLNode *inner = new_node->left;
    node->right = inner;
    if(inner) inner->parent =node;

    new_node->parent = parent;
    //new node node
    new_node->left  = node;
    node->parent = new_node;

    avl_update(node);
    avl_update(new_node);
    return new_node;
}

static AVLNode *rot_right(AVLNode *node){
    AVLNode *parent = node->parent;
    AVLNode *new_node = node->left;
    AVLNode *inner = new_node->right;

    node->left = inner;
    if(inner) inner->parent =node;

    new_node->parent = parent;
    new_node->right = node;
    node->parent = new_node;

    avl_update(node);
    avl_update(new_node);
    return new_node;
}

//if the left subtreee is taller by 2
static AVLNode *avl_fix_left(AVLNode *node){
    if(avl_height(node->left->left)<avl_height(node->left->right)) node->left = rot_left(node->left);
    return rot_right(node);
}

//if the rigth subtree is taller by 2
static AVLNode *avl_fix_right(AVLNode *node){
    if(avl_height(node->right->right)<avl_height(node->right->left)) node->right = rot_right(node->right);
    return rot_left(node);
}

//fix the imbalanced nodes and maintain the invariants untill the root s reached so that any imbalances in the dynamic tree will be fixed so far 
AVLNode *avl_fix(AVLNode *node){
    while(true){
        AVLNode **from = &node; //save the fixes subtree here 
        AVLNode *parent = node->parent;
        if(parent) from = parent->left == node ? &parent->left : &parent->right;

        avl_update(node);
        //fixing the height difference of 2 
        uint32_t  l = avl_height(node->left);
        uint32_t r = avl_height(node->right);
        if(l == r+2) *from = avl_fix_left(node);
        else if(r==l+2) *from = avl_fix_right(node);

        if(!parent) return *from;

        node = parent;
    }
    //after fixing the complete tree it will return the root of the given tree so that it will be easy to be verified 
}
//detach a node where one of its children is empty 
static AVLNode *avl_del_easy(AVLNode *node){
    assert(!node->left || !node->right);
    AVLNode *child = node->left ? node->left :  node->right;
    AVLNode *parent = node->parent;
    if(child) child->parent = parent;
    if(!parent) return child;

    AVLNode **from = parent->left == node ? &parent->left : &parent->right;
    *from = child;
    //after the delte fix any imbalances in the tree
    return avl_fix(parent);
}
//detach a node and return the nwew root of te node 
AVLNode *avl_del(AVLNode *node){
    //the easy case of 0 or 1 child is bein handles with the above functio 
    if(!node->left || !node->right) return avl_del_easy(node);

    //find the successor 
    AVLNode *victim= node->right;
    while(victim->left){
        victim = victim->left;
    }
    //detach the successor 
    AVLNode *root = avl_del_easy(victim);
    *victim = *node; //swapping with the sucessor 
    if(victim->left) victim->left->parent = victim;
    if(victim->right) victim->right->parent = victim;

    //attach the successor to the parent or update tje root pointer
    AVLNode  **from = &root;
    AVLNode *parent = node->parent;
    if(parent) from = parent->left == node  ? &parent->left : &parent->right;

    *from = victim;
    return root;
}

//now the offset into the succeeding or preceeding node which means the node that is a certaon steps away from the node by the traversal
AVLNode *avl_offset(AVLNode *node,int64_t offset){
    int64_t pos = 0; //this indicates the rank difference with the starting node
    while(offset != pos){
        if(pos<offset && pos+avl_cnt(node->right) >=offset){
            //this means that the target is inside the right subtree
            node=node->left;
            pos+= avl_cnt(node->left)+1;
        }else if(pos>offset && pos-avl_cnt(node->left)<=offset){
            //this means the target node is inside the left subree
            node = node->left;
            pos-= avl_cnt(node->right)+1;
        }else{
            //go to the parent 
            AVLNode *parent = node->parent;
            if(!parent) return NULL;

            if(node->right == node) pos-= avl_cnt(node->left)+1;
            else pos+=avl_cnt(node->right)+1;
        }
    }
    return node;
}



