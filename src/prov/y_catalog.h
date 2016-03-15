//
// Created by bon on 15-6-5.
//

#ifndef PG_PROV_Y_CATALOG_H
#define PG_PROV_Y_CATALOG_H

#include <utils/memutils.h>
#include "y_schemas.h"

/*
 * y_catalog_xxx
 * get the object xxx by its oid
 * first checking whether xxx for oid exists in schema
 * if exists get it from schema and return
 * if not ,query in pg_catalog by oid getting xxx
 * put xxx in schema and return
 * */

y_relation_entry*   y_catalog_relation(Oid o,MemoryContext cxt,y_schemas* sc);
y_data_type*        y_catalog_data_type(Oid o,MemoryContext cxt,y_schemas* sc);
y_namespace*        y_catalog_namespace(Oid o,MemoryContext cxt,y_schemas* sc);
y_proc*             y_catalog_proc(Oid o,MemoryContext cxt,y_schemas* sc);
y_op*               y_catalog_op(Oid o,MemoryContext cxt,y_schemas* sc);
char                y_data_type_category(Oid o);

#endif //PG_PROV_Y_CATALOG_H
