//
// Created by bon on 15-6-3.
//

#ifndef PG_PROV_Y_SCHEMAS_H
#define PG_PROV_Y_SCHEMAS_H

#include "postgres.h"
#include "utils/rbt/red_black_tree.h"
#include "c.h"
#include "nodes/primnodes.h"
#include <nodes/parsenodes.h>

typedef enum
{
    yT_invalid,

    yT_data_type,
    yT_column_entry,
    yT_relation_entry,
    yT_namespace,
    yT_proc,
    yT_op


}y_NodeTag;

typedef struct
{
    NodeTag     tag;//always should be T_invalid
    y_NodeTag   y_tag;
}y_node;


#define y_node_tag(nodePtr)     (((y_node*)nodePtr)->y_tag)
#define y_is_tag(nodePtr,tag_)   \
(((y_node*)nodePtr)->tag==T_Invalid && y_node_tag(nodePtr)==tag_)

typedef struct
{
    NodeTag     tag;
    y_NodeTag   y_tag;

    Oid         oid;
    const char* name;
}y_namespace;

typedef struct
{
    NodeTag     tag;
    y_NodeTag   y_tag;

    Oid         oid_pg_type;
    const char* name;
    const char* namespace;
    short       type_len;
}y_data_type;

typedef struct
{
    NodeTag     tag;
    y_NodeTag   y_tag;

    const char*     name;
    char            kind;//'l', 'r', or 'b'
}y_op;

//FIX ME
typedef struct
{
    NodeTag tag;
    y_NodeTag y_tag;

    const char*     namespace;
    const char*     name;
    int             args_cnt;//all params
    const char**    args_name;
    y_data_type**   args_type;
    char*           args_mode;//is null if all args is IN,'i' fot IN,'o' for OUT,'b' for INOUT ...
    y_data_type*    result_type;
    char            is_agg;
    char            rs_is_set;
}y_proc;

struct y_relation_entry_t;
typedef struct
{
    NodeTag     tag;
    y_NodeTag   y_tag;

    const char*     name;
    y_data_type*    data_type;
    struct y_relation_entry_t* rel;
}y_column_entry;

typedef struct y_relation_entry_t
{
    NodeTag     tag;
    y_NodeTag   y_tag;

    Oid         oid_class;
    char        kind;//'v' for view,'r' for base table,'t' for temp table
    const char* name;
    const char* namespace;

    size_t          col_cnt;
    y_column_entry* col_entries;
}y_relation_entry;



#define y_relation_entry_init(ptr)  \
    memset(ptr,0,sizeof(y_relation_entry));    \
    (ptr)->tag=T_Invalid; \
    (ptr)->y_tag=yT_relation_entry

#define y_column_entry_init(ptr)  \
    memset(ptr,0,sizeof(y_column_entry));    \
    (ptr)->tag=T_Invalid; \
    (ptr)->y_tag=yT_column_entry

#define y_data_type_init(ptr)  \
    memset(ptr,0,sizeof(y_data_type));    \
    (ptr)->tag=T_Invalid; \
    (ptr)->y_tag=yT_data_type

#define y_namespace_init(ptr)  \
    memset(ptr,0,sizeof(y_namespace));    \
    (ptr)->tag=T_Invalid; \
    (ptr)->y_tag=yT_namespace

#define y_proc_init(ptr)  \
    memset(ptr,0,sizeof(y_proc));    \
    (ptr)->tag=T_Invalid; \
    (ptr)->y_tag=yT_proc

#define y_op_init(ptr)  \
    memset(ptr,0,sizeof(y_op));    \
    (ptr)->tag=T_Invalid; \
    (ptr)->y_tag=yT_op

typedef struct
{
    rb_red_blk_tree* data_types;
    rb_red_blk_tree* relation_entries;
    rb_red_blk_tree* procs;
    rb_red_blk_tree* ops;
    rb_red_blk_tree* others;
}y_schemas;

int y_schemas_init(y_schemas*);
void y_schemas_destroy(y_schemas*);

y_relation_entry*   y_get_relation_entry_in_schemas(y_schemas*,Oid);
y_data_type*        y_get_data_type_in_schemas(y_schemas*,Oid);
y_proc*             y_get_proc_in_schemas(y_schemas*,Oid);
y_op*               y_get_op_in_schemas(y_schemas*,Oid);
void*               y_get_obj_in_schemas(y_schemas*,Oid);

int y_add_relation_entry_to_schemas(y_schemas*,Oid,y_relation_entry*);
int y_add_data_type_to_schemas(y_schemas*,Oid,y_data_type*);
int y_add_proc_to_schemas(y_schemas*,Oid,y_proc*);
int y_add_op_to_schemas(y_schemas*,Oid,y_op*);
int y_add_obj_to_schemas(y_schemas*,Oid,void*);

const char* y_const_to_str(Const* con);

#endif //PG_PROV_Y_SCHEMAS_H
