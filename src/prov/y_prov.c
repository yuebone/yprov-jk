//
// Created by bon on 15-6-1.
//
#include "y_prov.h"

y_conf yconf=
        {
                .opened=0,
                .data=NULL
        };

void* yalloc_lt(size_t sz)
{
    return MemoryContextAlloc(yconf.mem_cxt,sz);
}

int y_prov_init(y_prov* p)
{
    y_schemas_init(&p->schemas);
    p->cxt=AllocSetContextCreate(NULL,"prov once cxt",
                                 Y_ONCE_PROV_MEM_MIN,
                                 Y_ONCE_PROV_MEM_INIT,
                                 Y_ONCE_PROV_MEM_MAX);
	return 0;
}
void y_prov_destroy(y_prov* p)
{
    y_schemas_destroy(&p->schemas);
    MemoryContextDelete(p->cxt);
}