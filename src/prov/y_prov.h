//
// Created by bon on 15-6-1.
//

#ifndef PG_PROV_Y_PROV_H
#define PG_PROV_Y_PROV_H

#include <postgres.h>
#include <fmgr.h>
#include <utils/memutils.h>
#include <nodes/parsenodes.h>
#include "utils/y_str.h"
#include "y_schemas.h"
#include "y_catalog.h"
#include "y_save.h"

#ifndef Y_FILE_PATH_MAX
#include <stdio.h>
#define Y_FILE_PATH_MAX FILENAME_MAX
#endif

#ifndef Y_EXTENSION_NAME
#define Y_EXTENSION_NAME "yprov"
#endif

#ifndef Y_PROV_MEM_INIT
#define Y_PROV_MEM_INIT (1024*1024)
#endif
#ifndef Y_PROV_MEM_MIN
#define Y_PROV_MEM_MIN (1024)
#endif
#ifndef Y_PROV_MEM_MAX
#define Y_PROV_MEM_MAX (1024*1024*512)
#endif

#ifndef Y_ONCE_PROV_MEM_INIT
#define Y_ONCE_PROV_MEM_INIT (1024*32)
#endif
#ifndef Y_ONCE_PROV_MEM_MIN
#define Y_ONCE_PROV_MEM_MIN (32)
#endif
#ifndef Y_ONCE_PROV_MEM_MAX
#define Y_ONCE_PROV_MEM_MAX (1024*1024)
#endif


typedef struct
{
    y_str*     prov_path;
    y_str*     tree_path;
#ifdef Y_PRINT_ON
    y_str*     print_path;
#endif
    void*      data;
    char       opened;

    MemoryContext   mem_cxt;//long-term memory
}y_conf;

extern y_conf yconf;

extern void* yalloc_lt(size_t);//alloc long-term memory

typedef struct
{
    y_schemas   schemas;
    MemoryContext cxt;
}y_prov;

int     y_prov_init(y_prov* p);
void    y_prov_destroy(y_prov* p);

#define y_ALLOC(p,size)             MemoryContextAlloc(&(p)->cxt,size)
#define y_ALLOC_TYPE(p,_type_)      ((_type_*)MemoryContextAlloc(&(p)->cxt,sizeof(_type_)))
#define y_ALLOC_ARRAY(p,_type_,cnt) ((_type_*)MemoryContextAlloc(&(p)->cxt,sizeof(_type_)*cnt))

#define y_relation_byOid(p,o)       y_catalog_relation(o,(p)->cxt,&(p)->schemas)
#define y_data_type_byOid(p,o)      y_catalog_data_type(o,(p)->cxt,&(p)->schemas)
#define y_proc_byOid(p,o)           y_catalog_proc(o,(p)->cxt,&(p)->schemas)
#define y_op_byOid(p,o)             y_catalog_op(o,(p)->cxt,&(p)->schemas)




#endif //PG_PROV_Y_PROV_H
