//
// Created by bon on 15-11-14.
//

#ifndef PG_PROV_Y_UTILS_H
#define PG_PROV_Y_UTILS_H

#include <stddef.h>

/*
*------------------------------ stamrt ptr ---------------------------------
*/

typedef struct
{
    unsigned	cnt;
}y_shared_ptr_t;

#define y_shared_cnt(ptr)	( (((y_shared_ptr_t*)(ptr))-1)->cnt )
#define y_shared_ptr(ptr)   ( ((y_shared_ptr_t*)(ptr))-1 )

#define y_shared_alloc(_type_,name,sz,alloc_fn)  \
    _type_* name;\
{   \
    y_shared_ptr_t* sp = palloc(sz + sizeof(y_shared_ptr_t));    \
    sp->cnt = 1;    \
    name = (_type_*)(sp+1); \
}

static inline void* y_shared_cpy(void* ptr)
{
    ++y_shared_cnt(ptr);
    return ptr;
}

#define y_shared_free(ptr,free_fn)  \
{   \
    y_shared_ptr_t* sp = ((y_shared_ptr_t*)(ptr)) - 1;    \
    --sp->cnt;  \
    if (sp->cnt == 0)   \
        free_fn(sp);    \
}


#endif //PG_PROV_Y_UTILS_H
