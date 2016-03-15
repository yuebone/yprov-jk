//
// Created by bon on 15-6-5.
//

#include <postgres.h>
#include <funcapi.h>
#include "y_catalog.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_operator.h"

#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
#include "y_schemas.h"

/*
    HeapTuple tup=SearchSysCache1(,ObjectIdGetDatum(o));
    if(HeapTupleIsValid(tup))
    {
    ReleaseSysCache(tup);
    }
    else
        return 0;
 */

#define y_alloc(cxt,size)             MemoryContextAlloc(cxt,size)
#define y_alloc_type(cxt,_type_)      ((_type_*)MemoryContextAlloc(cxt,sizeof(_type_)))
#define y_alloc_array(cxt,_type_,cnt) ((_type_*)MemoryContextAlloc(cxt,sizeof(_type_)*cnt))

static inline char* y_alloc_cpy_cstr(MemoryContext cxt,const char* cstr)
{
    size_t len=strlen(cstr);
    char* str=y_alloc(cxt,len);
    memcpy(str,cstr,len+1);
    return str;
}

y_namespace* y_catalog_namespace(Oid o,MemoryContext cxt,y_schemas* sc)
{
    if(OidIsValid(o))
        return 0;
    y_namespace* n=y_get_obj_in_schemas(sc,o);
    if(n)
        return n;

    n=y_alloc_type(cxt,y_namespace);
    y_namespace_init(n);
    n->oid=o;
    n->name=y_alloc_cpy_cstr(cxt,get_namespace_name(o));

    y_add_obj_to_schemas(sc,o,n);
    return n;
}

inline const char* y_namespaceName(Oid o,MemoryContext cxt,y_schemas* sc)
{
    y_namespace *yNamespace=y_catalog_namespace(o,cxt,sc);
    if(yNamespace)
        return yNamespace->name;
    else
        return 0;
}

y_op* y_catalog_op(Oid o,MemoryContext cxt,y_schemas* sc)
{
    y_op *op;

    op = y_get_op_in_schemas(sc, o);
    if (op)
        return op;

    HeapTuple tup=SearchSysCache1(OPEROID,ObjectIdGetDatum(o));
    if(HeapTupleIsValid(tup))
    {

        Form_pg_operator pg_op=(Form_pg_operator)GETSTRUCT(tup);
        op=y_alloc_type(cxt,y_op);
        y_op_init(op);
        op->kind=pg_op->oprkind;
        op->name=y_alloc_cpy_cstr(cxt,pg_op->oprname.data);

        ReleaseSysCache(tup);

        y_add_op_to_schemas(sc,o,op);
        return op;
    }
    else
        return 0;
}

//FIX ME
y_proc* y_catalog_proc(Oid o,MemoryContext cxt,y_schemas* sc)
{
    y_proc* proc;

    proc=y_get_proc_in_schemas(sc,o);
    if(proc)
        return proc;

    HeapTuple tup=SearchSysCache1(PROCOID,ObjectIdGetDatum(o));
    if(HeapTupleIsValid(tup))
    {
        Oid result_type_oid;
        Oid arg_oid;
        int i;
        Oid		   *p_argtypes;
        char	  **p_argnames;
        char	   *p_argmodes;
        Form_pg_proc pg_proc = (Form_pg_proc) GETSTRUCT(tup);
        proc=y_alloc_type(cxt,y_proc);
        y_proc_init(proc);

        proc->args_cnt=get_func_arg_info(tup,&p_argtypes,&p_argnames,&p_argmodes);
        proc->name=y_alloc_cpy_cstr(cxt,pg_proc->proname.data);
        proc->namespace=y_namespaceName(o,cxt,sc);
        proc->is_agg=pg_proc->proisagg;
        proc->rs_is_set=pg_proc->proretset;
        result_type_oid=pg_proc->prorettype;
        proc->result_type=y_catalog_data_type(result_type_oid,cxt,sc);

        proc->args_type=y_alloc_array(cxt,y_data_type*,proc->args_cnt);
        proc->args_name=y_alloc_array(cxt,const char*,proc->args_cnt);
        if(p_argmodes)
        {
            proc->args_mode=y_alloc_array(cxt,char,proc->args_cnt);
            memcpy(proc->args_mode,p_argmodes,proc->args_cnt);
        }
        else
            proc->args_mode=0;


        for(i=0;i<proc->args_cnt;++i)
        {
            arg_oid=pg_proc->proargtypes.values[i];
            proc->args_type[i]=y_catalog_data_type(arg_oid,cxt,sc);
        }

        char argName[32]={'a','r','g',};
        if(p_argnames)
        {
            for(i=0;i<proc->args_cnt;++i)
                if(p_argnames[i])
                    proc->args_name[i]=y_alloc_cpy_cstr(cxt,p_argnames[i]);
                else
                {
                    snprintf(argName+3,32-3,"%d",i+1);
                    proc->args_name[i]=y_alloc_cpy_cstr(cxt,argName);
                }
        }
        else
        {
            for(i=0;i<proc->args_cnt;++i)
            {
                snprintf(argName+3,32-3,"%d",i+1);
                proc->args_name[i]=y_alloc_cpy_cstr(cxt,argName);
            }
        }

        if(p_argmodes)
            pfree(p_argmodes);
        if(p_argnames)
            pfree(p_argnames);
        pfree(p_argtypes);
        ReleaseSysCache(tup);

        y_add_proc_to_schemas(sc,o,proc);
        return proc;
    }
    else
        return 0;
}

static void y_catalog_column(Oid t,AttrNumber i,y_column_entry* ce,
                             MemoryContext cxt,y_schemas* sc)
{
    HeapTuple	tup;
    tup = SearchSysCache2(ATTNUM, ObjectIdGetDatum(t), Int16GetDatum(i));
    if(HeapTupleIsValid(tup))
    {
        Form_pg_attribute pg_att = (Form_pg_attribute) GETSTRUCT(tup);
        ce->name=y_alloc_cpy_cstr(cxt,pg_att->attname.data);
        ce->data_type=y_catalog_data_type(pg_att->atttypid,cxt,sc);

        /**!attention!***/
        ReleaseSysCache(tup);
    }
}

y_relation_entry* y_catalog_relation(Oid o,MemoryContext cxt,y_schemas* sc)
{
    y_relation_entry* re;

    re=y_get_relation_entry_in_schemas(sc,o);

    if(re)
        return re;
    else
    {
        HeapTuple tup;

        tup = SearchSysCache1(RELOID, ObjectIdGetDatum(o));
        if (HeapTupleIsValid(tup))
        {
            Form_pg_class pg_re = (Form_pg_class) GETSTRUCT(tup);
            re = y_alloc_type(cxt, y_relation_entry);
            int i;

            y_relation_entry_init(re);
            re->oid_class = o;
            re->kind = pg_re->relkind;
            //FIX ME
            if(pg_re->relpersistence=='t')
                re->kind='t';
            re->name = y_alloc_cpy_cstr(cxt, pg_re->relname.data);
            re->namespace=y_namespaceName(o,cxt,sc);
            re->col_cnt = pg_re->relnatts;

            re->col_entries = y_alloc_array(cxt, y_column_entry, re->col_cnt);
            i = 0;
            for (i = 0; i < re->col_cnt; ++i)
            {
                y_column_entry *ce = &re->col_entries[i];
                y_column_entry_init(ce);
                y_catalog_column(o, i + 1, ce, cxt, sc);
                ce->rel=re;
            }

            /**!attention!***/
            ReleaseSysCache(tup);

            y_add_relation_entry_to_schemas(sc,o,re);
            return re;
        }
        else
            return 0;
    }
}

y_data_type* y_catalog_data_type(Oid o,MemoryContext cxt,y_schemas* sc)
{
    y_data_type* ty=y_get_data_type_in_schemas(sc,o);
    if(ty)
        return ty;
    else
    {
        HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(o));
        if (HeapTupleIsValid(tup))
        {
            Form_pg_type pg_ty = (Form_pg_type) GETSTRUCT(tup);
            ty = y_alloc_type(cxt, y_data_type);

            y_data_type_init(ty);
            ty->name = y_alloc_cpy_cstr(cxt, pg_ty->typname.data);
            ty->type_len = pg_ty->typlen;
            ty->namespace=y_namespaceName(o,cxt,sc);

            /**!attention!***/
            ReleaseSysCache(tup);

            y_add_data_type_to_schemas(sc,o,ty);
            return ty;
        }
        else
            return 0;
    }
}

char y_data_type_category(Oid o)
{
    HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(o));
    if (HeapTupleIsValid(tup))
    {
        char r;
        Form_pg_type pg_ty = (Form_pg_type) GETSTRUCT(tup);
        r= pg_ty->typcategory;
        /**!attention!***/
        ReleaseSysCache(tup);
        return r;
    }
    else
        return 0;
}
