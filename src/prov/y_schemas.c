//
// Created by bon on 15-6-3.
//

#include "y_schemas.h"
#include <utils/lsyscache.h>
#include <fmgr.h>

#define Y_CONST_LEN 1024

static int _y_oid_comp(const void* a,const void* b)
{
    Oid oa=(Oid)a;
    Oid ob=(Oid)b;

    if(oa>ob)
        return 1;
    if(oa<ob)
        return -1;
    return 0;
}


int y_schemas_init(y_schemas* s)
{
    //FIX
    s->data_types=RBTreeCreate(_y_oid_comp,NullFunction,NullFunction,0,0);
    s->relation_entries=RBTreeCreate(_y_oid_comp,NullFunction,NullFunction,0,0);
    s->ops=RBTreeCreate(_y_oid_comp,NullFunction,NullFunction,0,0);
    s->procs=RBTreeCreate(_y_oid_comp,NullFunction,NullFunction,0,0);

    return 0;
}
void y_schemas_destroy(y_schemas* s)
{
    RBTreeDestroy(s->data_types);
    RBTreeDestroy(s->relation_entries);
    RBTreeDestroy(s->ops);
    RBTreeDestroy(s->procs);
}

#define Y_DEFINE_SCHEMA_OBJ(_type_,name,rbName)  \
_type_* y_get_##name##_in_schemas(y_schemas* s,Oid o)    \
{   \
    rb_red_blk_node* node=RBExactQuery(s->rbName,(void*)o);   \
    if(node)    \
        return (_type_*)(node->info); \
    return 0;   \
}   \
    \
int y_add_##name##_to_schemas(y_schemas* s,Oid o,_type_* v) \
{   \
    if(RBTreeInsert(s->rbName,(void*)o,v))   \
        return 0;   \
    return -1;  \
}


y_relation_entry* y_get_relation_entry_in_schemas(y_schemas* s,Oid o)
{
    rb_red_blk_node* node=RBExactQuery(s->relation_entries,(void*)o);
    if(node)
        return (y_relation_entry*)(node->info);
    return 0;
}

int y_add_relation_entry_to_schemas(y_schemas* s,Oid o,y_relation_entry* r)
{
    if(RBTreeInsert(s->relation_entries,(void*)o,r))
        return 0;
    return -1;
}

Y_DEFINE_SCHEMA_OBJ(y_data_type,data_type,data_types)
Y_DEFINE_SCHEMA_OBJ(y_proc,proc,procs)
Y_DEFINE_SCHEMA_OBJ(y_op,op,ops)
Y_DEFINE_SCHEMA_OBJ(void,obj,others)

const char* y_const_to_str(Const* con)
{
    int16		typlen;
    bool		typbyval;
    char		typalign;
    char		typdelim;
    Oid         paramOid;
    Oid         funcOid;

    get_type_io_data(con->consttype,IOFunc_output,
                     &typlen,&typbyval,&typalign,&typdelim,
                     &paramOid,&funcOid);

    return OidOutputFunctionCall(funcOid,con->constvalue);
}
