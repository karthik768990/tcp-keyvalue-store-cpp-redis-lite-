#pragma once

#include <stddef.h>

struct DList{
    DList *prev = NULL;
    DList *next = NULL;
};

//if the next node of the node is the node itself then it is empty 
inline bool dlist_empty(DList *node){
    return node->next ==node;
}
//making a simple cirucllar loop with it 
inline void dlist_init(DList *node){
    node->prev = node->next = node;
}
//detaching the node from the dlist 
inline void dlist_detach(DList *node){
    DList *prev = node->prev;
    DList *next = node->next;
    next->prev = prev;
    prev->next = next;
}
//this  function inserts the rookie before the target 
inline void dlist_insert_before(DList *target,DList *rookie){
    DList *prev = target->prev;
    prev->next = rookie;
    rookie->prev = prev;
    rookie->next = target;
    target->prev = rookie;
}