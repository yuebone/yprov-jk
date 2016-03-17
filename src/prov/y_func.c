//
// Created by bon on 15-6-2.
//
#include <postgres.h>
#include <fmgr.h>
#include <utils/guc.h>
#include <tcop/tcopprot.h>
#include <tcop/utility.h>
#include <parser/analyze.h>
#include <executor/executor.h>
#include "prov/y_prov.h"
#include "utils/y_io.h"
#include "utils/y_str.h"
#include "y_prov.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PGDLLEXPORT Datum yprov_open(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum yprov_close(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(yprov_open);;
PG_FUNCTION_INFO_V1(yprov_close);

extern void* y_prov_data_init(void*);
extern void  y_prov_data_fini(void*);

static void y_post_parse_analyze_hook(ParseState *pstate, Query *query);

extern post_parse_analyze_hook_type post_parse_analyze_hook;
static post_parse_analyze_hook_type yPrev_post_parse_analyze_hook;

extern ExecutorFinish_hook_type ExecutorFinish_hook;
static ExecutorFinish_hook_type yPre_ExecutorFinish_hook;
static void y_ExecutorFinish_hook(QueryDesc *queryDesc);

extern ProcessUtility_hook_type ProcessUtility_hook;
static ProcessUtility_hook_type yPrev_ProcessUtility_hook;
static void y_ProcessUtility_hook(Node *parsetree,
                                       const char *queryString, ProcessUtilityContext context,
                                       ParamListInfo params,
                                       DestReceiver *dest, char *completionTag);

static Query* y_query;
static y_prov y_prov_once;
static char y_status;

static void y_MemoryContextCallbackFunction(void* arg)
{
}
static MemoryContextCallback y_xcb={y_MemoryContextCallbackFunction,0,0};

static void _y_open()
{
    yconf.mem_cxt=AllocSetContextCreate(NULL,"prov mem cxt",
                                        Y_PROV_MEM_MIN,
                                        Y_PROV_MEM_INIT,
                                        Y_PROV_MEM_MAX);
    MemoryContextRegisterResetCallback(yconf.mem_cxt,&y_xcb);

    y_hook_str_alloc_func(yalloc_lt);
    y_hook_str_free_func(pfree);

    const char* conf = GetConfigOption(Y_EXTENSION_NAME".dir",1,0);
    size_t len=strlen(conf);
    if(conf[len-1]=='/')
    {
        yconf.prov_path=y_str_alloc_cpy_c2(conf,len);
    }
    else
    {
        yconf.prov_path=y_str_alloc_cpy_args("cc",conf,"/");
    }

    yconf.print_path=y_str_alloc_cpy_args("yc",yconf.prov_path,"print.txt");
    yconf.tree_path=y_str_alloc_cpy_args("yc",yconf.prov_path,"tree.txt");

#ifdef Y_PRINT_ON
    Y_PRINT_OPEN(yconf.print_path);
#endif

    y_hook_str_alloc_func(palloc);

    yconf.data=y_prov_data_init(0);
    yconf.opened=1;

    yPrev_post_parse_analyze_hook=post_parse_analyze_hook;
    post_parse_analyze_hook=y_post_parse_analyze_hook;
    y_status=0;

    Y_PRINT0("yprov opened\n");
}

static void _y_close()
{
    y_prov_data_fini(yconf.data);
    yconf.opened=0;
#ifdef Y_PRINT_ON
    Y_PRINT_CLOSE;
    y_str_destroy(yconf.print_path);
#endif

    post_parse_analyze_hook=yPrev_post_parse_analyze_hook;
    y_status=0;

    MemoryContextDelete(yconf.mem_cxt);

    Y_PRINT0("yprov closed\n");
}

void _PG_fini(void)
{
    if(yconf.opened)
        _y_close();

}

/*
 * prov_open(prov_path text,data_path text,options text) returns integer
 * */
Datum
yprov_open(PG_FUNCTION_ARGS)
{
    if(yconf.opened)
    {
        elog(WARNING,"yprov has opened");
        PG_RETURN_INT32(0);
    }
    _y_open();

    PG_RETURN_INT32(0);
}

/*
 * prov_close() returns integer
 * */
Datum
yprov_close(PG_FUNCTION_ARGS)
{
    if(yconf.opened)
        _y_close();
    PG_RETURN_INT32(0);
}

//FIX ME : now only support "CREATE VIEW AS" and "CREATE TABLE AS" statements
static char y_check(Query* q)
{
    if(q->commandType != CMD_UTILITY)
        return 0;
    if(IsA(q->utilityStmt,ViewStmt) || IsA(q->utilityStmt,CreateTableAsStmt))
        return 1;
    if(IsA(q->utilityStmt,DropStmt))
    {
        DropStmt* dropStmt=(DropStmt*)q->utilityStmt;
        if(dropStmt->removeType==OBJECT_VIEW || dropStmt->removeType==OBJECT_TABLE)
            return 1;
    }
    return 0;
}

extern void _y_do_prov(y_prov* prov,y_prov_data d,Query* q);
static void y_post_parse_analyze_hook(ParseState *pstate, Query *query)
{
    if(!y_check(query))
        return;

    Y_PRINT0("y_post_parse_analyze_hook\n");

    y_prov_init(&y_prov_once);
    MemoryContext old_cxt=MemoryContextSwitchTo(y_prov_once.cxt);

    y_status='q';
    y_query=copyObject(query);
    if(y_query->utilityStmt && IsA(y_query->utilityStmt,ViewStmt))
    {
        ViewStmt* v=(ViewStmt*)y_query->utilityStmt;
        if(IsA(v->query,SelectStmt))//FIX ME : v->query can be other types
        {
            Node* select_stmt=v->query;
            post_parse_analyze_hook=yPrev_post_parse_analyze_hook;
            v->query=(Node*)parse_analyze(select_stmt,"",0,0);
            post_parse_analyze_hook=y_post_parse_analyze_hook;
        }
        y_status='v';
        yPrev_ProcessUtility_hook=ProcessUtility_hook;
        ProcessUtility_hook=y_ProcessUtility_hook;
    }
    else if(IsA(y_query->utilityStmt,DropStmt))
    {
        DropStmt* dropStmt=(DropStmt*)y_query->utilityStmt;
        if(dropStmt->removeType==OBJECT_VIEW || dropStmt->removeType==OBJECT_TABLE)
        {
            y_status='d';
            yPrev_ProcessUtility_hook=ProcessUtility_hook;
            ProcessUtility_hook=y_ProcessUtility_hook;
        }
    }
    else
    {
        yPre_ExecutorFinish_hook=ExecutorFinish_hook;
        ExecutorFinish_hook=y_ExecutorFinish_hook;
    }


    MemoryContextSwitchTo(old_cxt);
}

static void y_ExecutorFinish_hook(QueryDesc *queryDesc)
{
    if(y_status!='q')
        goto yexe_std;

    MemoryContext old_cxt=MemoryContextSwitchTo(y_prov_once.cxt);

    _y_do_prov(&y_prov_once,yconf.data,y_query);
    MemoryContextSwitchTo(old_cxt);
    y_prov_destroy(&y_prov_once);

    y_status='e';
    ExecutorFinish_hook=yPre_ExecutorFinish_hook;

yexe_std:
    standard_ExecutorFinish(queryDesc);
}

static void y_ProcessUtility_hook(Node *parsetree,
                                       const char *queryString, ProcessUtilityContext context,
                                       ParamListInfo params,
                                       DestReceiver *dest, char *completionTag)
{
    standard_ProcessUtility(parsetree,queryString,context,params,dest,completionTag);
    Y_PRINT1("before y_ProcessUtility_hook %c\n",y_status);

    if(y_status=='d')
    {
        DropStmt* dropStmt=(DropStmt*)(y_query->utilityStmt);
        ListCell* lc;
        foreach(lc,dropStmt->objects)
        {
            List* nameList=lfirst(lc);
            y_strBuf* name=y_strBuf_alloc(1024);
            ListCell* _lc=list_head(nameList);

            y_strBuf_append(name,"c",strVal(lfirst(_lc)));
            while (_lc=lnext(_lc))
            {
                y_strBuf_append(name,".c",strVal(lfirst(_lc)));
            }

            y_removeRel_tbl(yconf.data,y_strBuf_buf(name));
            y_strBuf_destroy(name);
            y_prov_data_saveAll(yconf.data);
        }
    }

    if(y_status=='v')
    {
        MemoryContext old_cxt=MemoryContextSwitchTo(y_prov_once.cxt);
        _y_do_prov(&y_prov_once,yconf.data,y_query);
        MemoryContextSwitchTo(old_cxt);
        y_prov_destroy(&y_prov_once);
    }

    Y_PRINT0("y_ProcessUtility_hook\n");
    y_status=0;
    ProcessUtility_hook=yPrev_ProcessUtility_hook;
}

/*
Linux:
 DROP FUNCTION y_pr(integer,text)
 CREATE FUNCTION y_pr(integer,text)
 RETURNS integer
 AS
 '/home/bon/proj/ypg/prov/build/libprov_func.so','y_pr'
 LANGUAGE C STRICT;

 DROP FUNCTION prov(text,text);
 CREATE FUNCTION prov(text,text)
 RETURNS integer
 AS
 '/home/bon/proj/ypg/prov/build/libprov_func.so','prov'
 LANGUAGE C;

Win:
 DROP FUNCTION y_pr(integer,text);
 CREATE FUNCTION y_pr(integer,text)
 RETURNS integer
 AS
 'H:/DB/Pg/prov/src/x64/Release/prov_func.dll','y_pr'
 LANGUAGE C STRICT;

 DROP FUNCTION prov(text,text);
 CREATE FUNCTION prov(text,text)
 RETURNS integer
 AS
 'H:/DB/Pg/prov/src/x64/Release/prov_func.dll','prov'
 LANGUAGE C STRICT;

 set follow-fork-mode child
 set follow-fork-mode parent

 */

