//
// Created by bon on 15-11-14.
//

#include "y_save.h"
#include "y_prov.h"
#include "utils/y_io.h"
#include "utils/json/cJSON.h"
#include "utils/y_utils.h"
#include "y_schemas.h"
#include <catalog/pg_type.h>

#define Y_SET_ALGEBRA_SYM(_name_,str)   \
const char* y_##_name_##Str_=str;

#define Y_SET_ALGEBRA_SYMX(_name_,str,strX) \
Y_SET_ALGEBRA_SYM(_name_,str)   \
const char* y_##_name_##StrX=strX;


Y_SET_ALGEBRA_SYMX(pi,"proj","Projection")
Y_SET_ALGEBRA_SYMX(sigma,"filter","Filter")
Y_SET_ALGEBRA_SYMX(alpha,"groupBy;","Group By")
Y_SET_ALGEBRA_SYMX(rho,"rename","Rename As")
Y_SET_ALGEBRA_SYMX(chi,"as"," AS ")
Y_SET_ALGEBRA_SYMX(prod,"prod","Product")
Y_SET_ALGEBRA_SYMX(lJoin,"lJoin","Left Join")
Y_SET_ALGEBRA_SYMX(rJoin,"rJoin","Right Join")
Y_SET_ALGEBRA_SYMX(fJoin,"fJoin","Full Join")
Y_SET_ALGEBRA_SYMX(nJoin,"nJoin","Natural Join")
Y_SET_ALGEBRA_SYMX(uJoin,"?join","?Join")
Y_SET_ALGEBRA_SYMX(join,"join","Join")
Y_SET_ALGEBRA_SYMX(and,"and","And")
Y_SET_ALGEBRA_SYMX(or,"or","Or")
Y_SET_ALGEBRA_SYMX(not,"not","Not")
Y_SET_ALGEBRA_SYMX(union,"union","Union")
Y_SET_ALGEBRA_SYMX(intersect,"intersect","Intersect")
Y_SET_ALGEBRA_SYMX(except,"except","Except")
Y_SET_ALGEBRA_SYMX(common,"common","Common")

#ifdef Y_NOT_USE_SYMX
    #define Y_SYMX_STR(_name_)   Y_SYM_STR(_name_)
#else
    #define Y_SYMX_STR(_name_)   y_##_name_##StrX
#endif

#define Y_KIND_REGULAR_TABLE_STR    "tbl"
#define Y_KIND_FUNCTION_STR         "FUNC"
#define Y_KIND_VALUES_STR           "VALUES"
#define Y_KIND_VIEW_STR             "view"
#define Y_KIND_TMP_TABLE_STR        "TMP"
#define Y_KIND_CTE_STR              "CTE"

/*
 * ---------------------------prov data-------------------------------
 *
 * status of json files update
 * stat.json:
 * {
 *   "id":id-begin,
 *   "relmap":relmap-file-length,
 *   "rel":rel-file-length,
 *   "trel":trel-file-length,
 *   "expr":expr-file-length,
 *   "cexpr":cexpr-file-length,
 *   "timestamp":timestamp
 * }
 *
 * name-id mapping of relation(table or view)
 * relmap.json:
 *  {
 *    "name":"id",
 *    ...
 *  }
 *
 * relation entity(regular table or view)
 * rel.json:
 *  {
 *    "id":{"name":name,"kind":kind,"cnt":col_cnt,
 *          "cols":[{"name":name,"type":type,"from":{"id":id}},...]},
 *    ...
 *  }
 *
 * temporarily relation entity(temporarily table or cte or function-call)
 * trel.json:
 *  {
 *    "id":{"name":name,"kind":kind,"cnt":col_cnt,"cols":[{"name":name,"type":type,"from":{"id":id}},...]},
 *    "id":{"name":name,"kind":"func","acnt":args_cnt,"rcnt":returns_cnt,
 *      "args":[{"info":info,"from":[{"id":id},...]},...],"returns":[{"name":name,"type":type},...]},
 *    ...
 *  }
 *
 *  expr.json:
 *  {
 *    "id":{"algebra":algebra,"info":[str,...],"from":[id,...]},
 *    ...
 *  }
 *
 *  cexpr.json:
 *  {
 *    "id":{"info":info,"from":[{"id":id,"idx":idx},...]},
 *    ...
 *  }
 *
 * */

typedef struct
{
    /*
     * using to check whether corresponding json obj has updated
     * */
    char stat_updated;
    char relmap_updated;
    char rel_updated;
    char trel_updated;
    char expr_updated;
    char cexpr_updated;

    FILE*  statF;
    FILE*  relmapF;
    FILE*  relF;
    FILE*  trelF;
    FILE*  exprF;
    FILE*  cexprF;

    cJSON*  statJ;
    cJSON*  relmapJ;
    cJSON*  relJ;
    cJSON*  trelJ;
    cJSON*  exprJ;
    cJSON*  cexprJ;

    int     idx;

    cJSON_Hooks jHook;

    y_strBuf* strBuf;
}y_prov_data_t;

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

#define _Y_STR(_name_,func,args...)     \
{       \
    y_str_alloc_func alloc_old=y_hook_str_alloc_func(yalloc_lt);    \
    (_name_)=func(args);    \
    y_hook_str_alloc_func(alloc_old);   \
}


#define _Y_DEFINE(_name_)   \
y_str* _name_##_path=y_str_alloc_cpy_args("yc",data_dir,#_name_".json");   \
FILE* _name_##F=0;  \
size_t _name_##_len

#define _Y_OPEN_FILE(_name_,mode)   \
_name_##F=y_std_fopen(y_str_ptr(_name_##_path),mode);   \
if(!_name_##F)  \
{   \
    errmsg="cannt open file "#_name_".json";    \
    goto data_err;  \
}

#define _Y_UPDATE_FILE_LEN_MAX(_file_)  \
{   \
    fseek(_file_##F,0,SEEK_END);    \
    _file_##_len=ftell(_file_##F);   \
    fseek(_file_##F,0,SEEK_SET);    \
    if(_file_##_len>((int)(0x7fff))-1) \
    {errmsg="file "#_file_".json too large";    \
     goto data_err;  }\
    if(file_lenMAX<_file_##_len)    \
        file_lenMAX=_file_##_len;   \
}

#define _Y_UPDATE_FILE_LEN_MAX2(_file_)  \
{   \
    _file_##_len=cJSON_GetObjectItem(data->statJ,#_file_)->valueint;   \
    if(_file_##_len>((int)(0x7fff))-1) \
    {errmsg="file "#_file_".json too large";    \
     goto data_err;  }\
    if(file_lenMAX<_file_##_len)    \
        file_lenMAX=_file_##_len;   \
}

#define _Y_GENERATE_JSON(_name_)    \
fread(buf,1,_name_##_len,_name_##F);    \
buf[_name_##_len]=0;    \
data->_name_##J=cJSON_Parse(buf)

#define _Y_GENERATE_JSON2(_name_)    \
fread(buf,1,_name_##_len,_name_##F);    \
buf[_name_##_len]=0;    \
buf[_name_##_len-1]='}';    \
buf[_name_##_len-2]='\n';    \
data->_name_##J=cJSON_Parse(buf);   \
fseek(_name_##F,_name_##_len-1,SEEK_SET)

#define _Y_INIT_FILE(_name_)    \
cJSON_AddNumberToObject(data->statJ,#_name_,12); \
data->_name_##J=cJSON_CreateObject(); \
fwrite("{\n\"0\":null\n}",1,12,_name_##F);   \
y_std_fflush(_name_##F);    \
fseek(_name_##F,11,SEEK_SET)

y_prov_data y_prov_data_init(void* arg)
{
    char data_init=1;
    long file_lenMAX=0;
    char* buf;
    y_prov_data_t* data;
    const char* errmsg=0;
    y_str_alloc_func alloc_old;

    alloc_old=y_hook_str_alloc_func(yalloc_lt);
    data=yalloc_lt(sizeof(y_prov_data_t));
    memset(data,0,sizeof(y_prov_data_t));
    y_str* data_dir=y_str_alloc_cpy_args("yc",yconf.prov_path,"data/");

    _Y_DEFINE(stat);
    _Y_DEFINE(relmap);
    _Y_DEFINE(rel);
    _Y_DEFINE(trel);
    _Y_DEFINE(expr);
    _Y_DEFINE(cexpr);

    Y_PRINT0("begin to load data\n");

    data->jHook.free_fn=pfree;
    data->jHook.malloc_fn=yalloc_lt;
    cJSON_InitHooks(&data->jHook);

    statF=y_std_fopen(y_str_ptr(stat_path),"r");
    if(!statF)
    {
        if(errno==ENOENT)//file not exist
        {
            data_init=0;
            statF=y_std_fopen(y_str_ptr(stat_path),"w");
            if(!statF)
            {
                errmsg="cannt create and open file stat.json";
                goto data_err;
            }
        }
        else
        {
            errmsg="cannt open file stat.json";
            goto data_err;
        }
    }

    if(data_init)
    {
        _Y_OPEN_FILE(relmap,"r+");
        _Y_OPEN_FILE(rel,"r+");
        _Y_OPEN_FILE(trel,"r+");
        _Y_OPEN_FILE(expr,"r+");
        _Y_OPEN_FILE(cexpr,"r+");

        Y_PRINT0("begin reading json file\n");

        /*get file len and update file_lenMAX*/
        _Y_UPDATE_FILE_LEN_MAX(stat);

        buf=palloc(file_lenMAX+1);
        if(!buf)
        {
            errmsg="palloc for file buf failed";
            goto data_err;
        }

        /*read json file and generate cJSON structure*/
        _Y_GENERATE_JSON(stat);

        Y_PRINT0("read stat\n");

        _Y_UPDATE_FILE_LEN_MAX2(relmap);
        _Y_UPDATE_FILE_LEN_MAX2(rel);
        _Y_UPDATE_FILE_LEN_MAX2(trel);
        _Y_UPDATE_FILE_LEN_MAX2(expr);
        _Y_UPDATE_FILE_LEN_MAX2(cexpr);

        Y_PRINT0("read relmap,rel,trel,expr,cexpr\n");

        buf=repalloc(buf,file_lenMAX+1);
        if(!buf)
        {
            errmsg="repalloc for file buf failed";
            goto data_err;
        }

        _Y_GENERATE_JSON2(relmap);
        _Y_GENERATE_JSON2(rel);
        _Y_GENERATE_JSON2(trel);
        _Y_GENERATE_JSON2(expr);
        _Y_GENERATE_JSON2(cexpr);

        pfree(buf);

        data->idx=cJSON_GetObjectItem(data->statJ,"idx")->valueint;

        Y_PRINT0("end reading json file\n");
    }
    else
    {
        _Y_OPEN_FILE(relmap,"w+");
        _Y_OPEN_FILE(rel,"w+");
        _Y_OPEN_FILE(trel,"w+");
        _Y_OPEN_FILE(expr,"w+");
        _Y_OPEN_FILE(cexpr,"w+");

        Y_PRINT0("begin to init data\n");
        data->statJ=cJSON_CreateObject();
        cJSON_AddNumberToObject(data->statJ,"idx",0);
        data->idx=0;
        _Y_INIT_FILE(relmap);
        _Y_INIT_FILE(rel);
        _Y_INIT_FILE(trel);
        _Y_INIT_FILE(expr);
        _Y_INIT_FILE(cexpr);

        buf=cJSON_Print(data->statJ);
        fwrite(buf,1,strlen(buf),statF);
        y_std_fflush(statF);
        pfree(buf);

        Y_PRINT0("end init data\n");
    }

    data->statF=statF;
    data->relmapF=relmapF;
    data->relF=relF;
    data->trelF=trelF;
    data->exprF=exprF;
    data->cexprF=cexprF;

    data->strBuf=y_strBuf_alloc(1024);

    goto data_end;

data_err:
    if(statF)   y_std_fclose(statF);
    if(relF)    y_std_fclose(relF);
    if(trelF)   y_std_fclose(trelF);
    if(exprF)   y_std_fclose(exprF);
    if(cexprF)  y_std_fclose(cexprF);

data_end:
    y_str_destroy(stat_path);
    y_str_destroy(relmap_path);
    y_str_destroy(rel_path);
    y_str_destroy(trel_path);
    y_str_destroy(expr_path);
    y_str_destroy(cexpr_path);
    y_str_destroy(data_dir);

    y_hook_str_alloc_func(alloc_old);

    if(errmsg)
    {
        elog(ERROR,errmsg);
        return 0;
    }
    Y_PRINT0("loading data done\n");

    return data;
}

void y_prov_data_fini(y_prov_data _data)
{
    y_prov_data_t* data=_data;

    y_prov_data_saveAll(data);
    y_strBuf_destroy(data->strBuf);

    y_std_fclose(data->statF);
    y_std_fclose(data->relmapF);
    y_std_fclose(data->relF);
    y_std_fclose(data->trelF);
    y_std_fclose(data->exprF);
    y_std_fclose(data->cexprF);

    cJSON_Delete(data->statJ);
    cJSON_Delete(data->relmapJ);
    cJSON_Delete(data->relJ);
    cJSON_Delete(data->trelJ);
    cJSON_Delete(data->exprJ);
    cJSON_Delete(data->cexprJ);
    pfree(data);

    Y_PRINT0("colse data\n");
}

#define _Y_FILE_SAVE(_name_)    \
if(data->_name_##_updated){ \
fpos_t pos; \
fgetpos(data->_name_##F,&pos);  \
fwrite("}",1,1,data->_name_##F); \
y_std_fflush(data->_name_##F);  \
long _name_##_len=ftell(data->_name_##F); \
cJSON_SetIntValue(cJSON_GetObjectItem(data->statJ,#_name_),_name_##_len);  \
data->stat_updated=1;   \
data->_name_##_updated=0;   \
fsetpos(data->_name_##F,&pos);  \
Y_PRINT0("do save "#_name_"\n");   \
}

void y_prov_data_saveAll(y_prov_data _data)
{
    y_prov_data_t* data=_data;

    _Y_FILE_SAVE(rel);
    _Y_FILE_SAVE(trel);
    _Y_FILE_SAVE(expr);
    _Y_FILE_SAVE(cexpr);

    if(data->relmap_updated)
    {
        y_str* path=y_str_alloc_cpy_args("yc",yconf.prov_path,"data/relmap.json");
        data->relmapF=y_std_refopen(y_str_ptr(path),"w",data->relmapF);

        //fix me
        char* buf=cJSON_Print(data->relmapJ);
        fwrite(buf,1,strlen(buf),data->relmapF);
        y_std_fflush(data->relmapF);
        long relmap_len=ftell(data->relmapF);

        data->relmap_updated=0;
        data->jHook.free_fn(buf);
        y_str_destroy(path);

        cJSON_SetIntValue(cJSON_GetObjectItem(data->statJ,"relmap"),relmap_len);
        data->stat_updated=1;
    }

    if(data->stat_updated)
    {
        y_str* path=y_str_alloc_cpy_args("yc",yconf.prov_path,"data/stat.json");
        data->statF=y_std_refopen(y_str_ptr(path),"w",data->statF);
        //fix me
        cJSON_SetIntValue(cJSON_GetObjectItem(data->statJ,"idx"),data->idx);
        char* buf=cJSON_Print(data->statJ);
        fseek(data->statF,0,SEEK_SET);
        fwrite(buf,1,strlen(buf),data->statF);
        y_std_fflush(data->statF);
        data->stat_updated=0;
        data->jHook.free_fn(buf);
        y_str_destroy(path);
    }
}

/*
 * ---------------------------prov data end-------------------------------
 * */

typedef enum
{
    yT_prov_rel,
    yT_prov_trel,
    yT_prov_expr,
    yT_prov_cexpr,
    yT_prov_column,
    yT_prov_cexpr_tmp
}y_node_type_t;

typedef struct
{
    y_node_type_t type;
    /*
     * content of y_prov_node_t depends on it's type,
     * which is identified by id
     * -------------:------------
     * id-prefix    :   node-type
     * -------------:------------
     * r            :   rel(view or regular)
     * t            :   temp rel(temp table,cte,func)
     * e            :   expr
     * c            :   cexpr
     * */
    //y_str*          id;
}y_prov_node_t;

typedef struct
{
    y_node_type_t type;

    y_str*  id;
    /*
     * content of expr node,
     * format:
     * expr.json:
     *  {
     *    "id":{"algebra":algebra,"info":[str,...],"from":[id,...]},
     *    ...
     *  }
     * */
    cJSON*  info;
    List*   from;
    char    saved;
}y_prov_expr;

typedef struct
{
    y_node_type_t type;

    y_str*  id;
    cJSON*  info;

    List* rangeNodes;/*list of y_tNode or y_cNode*/
    List* cols;/*list of y_prov_column*/

    char    saved;
}y_prov_rel,y_prov_trel;

typedef struct
{
    y_node_type_t type;

    y_str* id;/*id of trel or rel*/
    int idx;
    y_str* content;
    cJSON* info;
    List* from;/*list of y_cNode*/
}y_prov_column;

typedef struct
{
    y_node_type_t type;

    y_str*  content;
    char    is_composited;
    List*   from;/*list of y_cNode*/
    y_str*  id;
    cJSON*  info;
    char    saved;
}y_prov_cexpr,y_prov_cexpr_tmp;

#define Y_REL_NODE      "r"
#define Y_TREL_NODE     "t"
#define Y_EXPR_NODE     "e"
#define Y_CEXPR_NODE    "c"

#define y_prov_is(p,_type_) (((y_prov_node_t*)p)->type==yT_##_type_)

static y_str* y_cExprInfo(y_cNode n)
{
    if(y_prov_is(n,prov_column))
        return ((y_prov_column*)n)->content;
    if(y_prov_is(n,prov_cexpr))
        return ((y_prov_cexpr*)n)->content;
    if(y_prov_is(n,prov_cexpr_tmp))
        return ((y_prov_cexpr_tmp*)n)->content;
    return 0;
}

static void y_cExprInfoList(y_strBuf** bufP,List* nodes)
{
    ListCell *lc;
    lc=list_head(nodes);
    if(!lc)
        return;

    *bufP=y_strBuf_append(*bufP,"y",y_cExprInfo(lfirst(lc)));

    lc=lnext(lc);
    while (lc)
    {
        *bufP=y_strBuf_append(*bufP,"cy",",",y_cExprInfo(lfirst(lc)));
        lc=lnext(lc);
    }
}

static y_str* y_get_prov_id(void* p)
{
    y_prov_node_t* n=p;
    switch (n->type)
    {
        case yT_prov_rel:
        case yT_prov_trel:/*FIX ME*/
            return ((y_prov_rel*)p)->id;
        case yT_prov_expr:
            return ((y_prov_expr*)p)->id;
        case yT_prov_cexpr:
            return ((y_prov_cexpr*)p)->id;
        default:
            return 0;
    }
}

/*
#define _Y_DEBUG_PRINT_INFO(n,_type_)   \
{   \
    y_##_type_* _n=(y_##_type_*)(n);    \
    cJSON* j=_n->info;  \
    const char* str=cJSON_Print(j); \
    Y_PRINT2("%s\n%s\n",#_type_,str);   \
    pfree(str); \
}
*/
#define _Y_DEBUG_PRINT_INFO(n,_type_)
/*
 * ---------------------------obj alloc-------------------------------
 * */

#define Y_NEW_ID(p,type) \
int _id=++data->idx;    \
char id[32]={};         \
snprintf(id,32,"%s%d",type,_id);\
_Y_STR(p->id,y_str_alloc_cpy_c,id)

inline cJSON* _y_colInfo_alloc(const char* name,const char* type)
{
    /*
     * column info : {"name":name,"type":type}
     * */
    cJSON* c=cJSON_CreateObject();
    if(name)
        cJSON_AddStringToObject(c,"name",name);
    else
        cJSON_AddNullToObject(c,"name");
    if(type)
        cJSON_AddStringToObject(c,"type",type);
    else
        cJSON_AddNullToObject(c,"type");
    return c;

}

static y_prov_expr* _y_expr_alloc(y_prov_data_t* data,const char* desc,
                                    const char* e1,const char* e2)
{
    y_shared_alloc(y_prov_expr,p,sizeof(y_prov_expr),yalloc_lt);
    Y_NEW_ID(p,Y_EXPR_NODE);
    p->info=cJSON_CreateObject();
    p->type=yT_prov_expr;
    p->from=0;
    p->saved=0;
    cJSON_AddStringToObject(p->info,"algebra",desc);
    if(e1)
    {
        const char* eArr[2];
        eArr[0]=(e1);
        eArr[1]=(e2);
        cJSON* exprArr;
        if(e2)
            exprArr=cJSON_CreateStringArray(eArr,2);
        else
            exprArr=cJSON_CreateStringArray(eArr,1);
        cJSON_AddItemToObject(p->info,"info",exprArr);
    }
    else
        cJSON_AddNullToObject(p->info,"info");
    cJSON_AddItemToObjectCS(p->info,"from",cJSON_CreateArray());

    cJSON_AddItemToObject(data->exprJ,y_str_ptr(p->id),p->info);

    Y_PRINT2("alloc expr_node(%s,%s)\n",desc,y_str_ptr(p->id));

    return p;
}

static y_prov_rel* _y_rel_alloc(y_prov_data_t* data,
                                   const char* name,const char* kind,int cnt)
{
    y_shared_alloc(y_prov_rel,p,sizeof(y_prov_rel),yalloc_lt);
    Y_NEW_ID(p,Y_REL_NODE);
    p->info=cJSON_CreateObject();
    p->type=yT_prov_rel;
    p->saved=0;
    p->cols=0;
    p->rangeNodes=0;
    cJSON_AddStringToObject(p->info,"name",name);
    cJSON_AddStringToObject(p->info,"kind",kind);
    cJSON_AddNumberToObject(p->info,"cnt",cnt);
    cJSON_AddItemToObject(p->info,"from",cJSON_CreateArray());

    cJSON *mapJ;
    cJSON_AddItemToObject(data->relJ,y_str_ptr(p->id),p->info);
    mapJ=cJSON_GetObjectItem(data->relmapJ,name);
    if(mapJ)
        cJSON_ReplaceItemInObject(data->relmapJ,name,cJSON_CreateString(y_str_ptr(p->id)));
    else
        cJSON_AddItemToObject(data->relmapJ,name,cJSON_CreateString(y_str_ptr(p->id)));

    Y_PRINT2("alloc rel_node(%s,%s)\n",name,y_str_ptr(p->id));

    return p;
}

static y_prov_trel* _y_trel_alloc(y_prov_data_t* data,
                                const char* name,const char* kind,int cnt)
{
    y_shared_alloc(y_prov_trel,p,sizeof(y_prov_trel),yalloc_lt);
    Y_NEW_ID(p,Y_TREL_NODE);
    p->info=cJSON_CreateObject();
    p->type=yT_prov_trel;
    p->saved=0;
    p->cols=0;
    p->rangeNodes=0;
    if(name)
        cJSON_AddStringToObject(p->info,"name",name);
    else
        cJSON_AddNullToObject(p->info,"name");
    cJSON_AddStringToObject(p->info,"kind",kind);
    cJSON_AddNumberToObject(p->info,"cnt",cnt);
    cJSON_AddItemToObject(p->info,"from",cJSON_CreateArray());

    cJSON_AddItemToObject(data->trelJ,y_str_ptr(p->id),p->info);

    Y_PRINT2("alloc trel_node(%s,%s)\n",name,y_str_ptr(p->id));

    return p;
}

static y_prov_rel* _y_rel_get(y_prov_data_t* data,const char* name,y_relation_entry* re)
{
    cJSON* j;
    /*j: {"name":"id"}*/
    j=cJSON_GetObjectItem(data->relmapJ,name);
    if(j)//table named 'name' is in relmap
    {
        const char* id=j->valuestring;
        /*now j: {"id":{"desc","info":{column,...}}}*/
        j=cJSON_GetObjectItem(data->relJ,id);
        Assert(j);

        y_shared_alloc(y_prov_rel,n,sizeof(y_prov_rel),yalloc_lt);
        n->type=yT_prov_rel;
        _Y_STR(n->id,y_str_alloc_keep_cStr,id);
        n->info=j;
        n->saved=1;
        n->cols=0;
        n->rangeNodes=0;

        Y_PRINT2("alloc and get rel_node(%s,%s)\n",name,y_str_ptr(n->id));
        return n;
    }
    return 0;
}

static y_prov_trel* _y_func_alloc(y_prov_data_t* data,
                                   const char* name,int acnt,int rcnt)
{
    y_shared_alloc(y_prov_trel,p,sizeof(y_prov_trel),yalloc_lt);
    Y_NEW_ID(p,Y_TREL_NODE);
    p->info=cJSON_CreateObject();
    p->type=yT_prov_trel;
    p->rangeNodes=0;
    p->cols=0;
    p->saved=0;
    cJSON_AddStringToObject(p->info,"name",name);
    cJSON_AddStringToObject(p->info,"kind",Y_KIND_FUNCTION_STR);
    cJSON_AddNumberToObject(p->info,"acnt",acnt);
    cJSON_AddNumberToObject(p->info,"rcnt",rcnt);

    cJSON_AddItemToObject(data->trelJ,y_str_ptr(p->id),p->info);

    Y_PRINT2("alloc func_node(%s,%s)\n",name,y_str_ptr(p->id));

    return p;
}

static y_prov_column* _y_column_alloc()
{
    y_shared_alloc(y_prov_column,p,sizeof(y_prov_column),yalloc_lt);
    p->type=yT_prov_column;
    Y_PRINT0("alloc column\n");
    return p;
}

static y_prov_cexpr_tmp* _y_cexpr_tmp_alloc(y_str* content,char is_composite)
{
    size_t size=sizeof(y_prov_cexpr);
    y_shared_alloc(y_prov_cexpr_tmp,p,size,yalloc_lt);
    p->type=yT_prov_cexpr_tmp;
    p->content=content;
    p->is_composited=is_composite;
    p->from=0;
    Y_PRINT1("alloc cexpr_tmp(%s)\n",y_str_ptr(content));
    return p;
}

static void _y_fromTo_col(y_cNode fn,y_cNode tn);
static y_prov_cexpr* _y_tmp_to_cexpr(y_prov_data_t* data,y_prov_cexpr_tmp* tmp)
{
    ListCell* lc;
    List* raw_from=tmp->from;
    y_prov_cexpr* p=(y_prov_cexpr*)tmp;
    p->type=yT_prov_cexpr;
    Y_NEW_ID(p,Y_CEXPR_NODE);
    p->saved=0;
    p->from=0;
    p->info=cJSON_CreateObject();
    cJSON_AddStringToObject(p->info,"info",y_str_ptr(tmp->content));
    cJSON_AddItemToObject(p->info,"from",cJSON_CreateArray());
    foreach(lc,raw_from)
    {
        _y_fromTo_col(lfirst(lc),p);
    }

    cJSON_AddItemToObject(data->cexprJ,y_str_ptr(p->id),p->info);

    Y_PRINT1("tmp to cexpr(%s)\n",y_str_ptr(p->content));

    return p;
}

static void _y_node_destroy(y_prov_node_t* p)
{
    if(--y_shared_cnt(p)>0)
        return;

    switch (p->type)
    {
        case yT_prov_rel:
        case yT_prov_trel:/*FIX ME if y_prov_rel is different with yT_prov_trel*/
        {
            ListCell* lc;
            y_prov_rel* rel=(y_prov_rel*)p;
            Y_PRINT1("node destroy(%s)\n",y_str_ptr(rel->id));
            y_str_destroy(rel->id);
            if(rel->cols){foreach(lc,rel->cols)
                { _y_node_destroy(lfirst(lc)); }list_free(rel->cols);}
            if(rel->rangeNodes){foreach(lc,rel->rangeNodes)
                { _y_node_destroy(lfirst(lc)); }list_free(rel->rangeNodes);}
            break;
        }
        case yT_prov_expr:
        {
            ListCell* lc;
            y_prov_expr* expr=(y_prov_expr*)p;
            Y_PRINT1("node destroy(%s)\n",y_str_ptr(expr->id));
            y_str_destroy(expr->id);
            if(expr->from){foreach(lc,expr->from)
                { _y_node_destroy(lfirst(lc)); }list_free(expr->from);}
            break;
        }
        case yT_prov_cexpr:
        {
            ListCell* lc;
            y_prov_cexpr* expr=(y_prov_cexpr*)p;
            Y_PRINT2("node destroy(%s,%s)\n",y_str_ptr(expr->id),y_str_ptr(expr->content));
            y_str_destroy(expr->id);
            y_str_destroy(expr->content);
            if(expr->from){foreach(lc,expr->from)
                { _y_node_destroy(lfirst(lc)); }list_free(expr->from);}
            break;
        }

        case yT_prov_cexpr_tmp:
        {
            ListCell* lc;
            y_prov_cexpr_tmp* cexpr_tmp=(y_prov_cexpr_tmp*)p;
            Y_PRINT1("node destroy(%s)\n",y_str_ptr(cexpr_tmp->content));
            y_str_destroy(cexpr_tmp->content);
            if(cexpr_tmp->from){foreach(lc,cexpr_tmp->from)
                { _y_node_destroy(lfirst(lc)); }list_free(cexpr_tmp->from);}
            break;
        }
        case yT_prov_column:
        {
            ListCell* lc;
            y_prov_column* column=(y_prov_column*)p;
            Y_PRINT2("node destroy(%s,%d)\n",y_str_ptr(column->id),column->idx);
            y_str_destroy(column->id);
            y_str_destroy(column->content);
            if(column->from){foreach(lc,column->from)
                { _y_node_destroy(lfirst(lc)); }list_free(column->from);}
            break;
        }
        default:
            ;
    }

    y_shared_free(p,pfree);
}

/*
 * ---------------------------obj alloc end-------------------------------
 * */

/*
 * ---------------------------obj save-------------------------------
 * */

static inline void __y_save_node(y_prov_data_t* data,y_str* id,cJSON* info,FILE* fp)
{
    const char* infoStr=cJSON_Print(info);
    y_std_fprintf(fp,",\"%s\":%s\n",y_str_ptr(id),infoStr);
    data->jHook.free_fn((void*)infoStr);
}

static void _y_save_node(y_prov_data_t* data,void* n)
{
    ListCell* lc;
    if(y_prov_is(n,prov_rel))
    {
        y_prov_rel* rel=n;
        if(rel->saved)
            return;

        data->rel_updated=1;
        rel->saved=1;
        data->relmap_updated=1;

        Y_PRINT1("saving prov_rel(%s)\n",y_str_ptr(rel->id));
        __y_save_node(data,rel->id,rel->info,data->relF);

        foreach(lc,rel->rangeNodes)
        {
            _y_save_node(data,lfirst(lc));
            _y_node_destroy(lfirst(lc));
        }
        list_free(rel->rangeNodes);rel->rangeNodes=0;

        foreach(lc,rel->cols)
        {
            y_prov_column* column=lfirst(lc);
            ListCell* _lc2;
            foreach(_lc2,column->from)
            {
                _y_save_node(data,lfirst(_lc2));
                _y_node_destroy(lfirst(_lc2));
            }
            list_free(column->from);column->from=0;
        }
        Y_PRINT1("saved prov_rel(%s)\n",y_str_ptr(rel->id));
    }
    else if(y_prov_is(n,prov_trel))
    {
        y_prov_rel* trel=n;
        if(trel->saved)
            return;

        data->trel_updated=1;
        trel->saved=1;

        Y_PRINT1("saving prov_trel(%s)\n",y_str_ptr(trel->id));
        __y_save_node(data,trel->id,trel->info,data->trelF);

        foreach(lc,trel->rangeNodes)
        {
            _y_save_node(data,lfirst(lc));
            Y_PRINT0("de r\n");
            _y_node_destroy(lfirst(lc));
        }
        list_free(trel->rangeNodes);trel->rangeNodes=0;

        foreach(lc,trel->cols)
        {
            y_prov_column* column=lfirst(lc);
            ListCell* _lc;
            foreach(_lc,column->from)
            {
                _y_save_node(data,lfirst(_lc));
                Y_PRINT0("de c\n");
                _y_node_destroy(lfirst(_lc));
            }
            list_free(column->from);column->from=0;
        }
        Y_PRINT1("saved prov_trel(%s)\n",y_str_ptr(trel->id));
    }
    else if(y_prov_is(n,prov_expr))
    {
        y_prov_expr* expr=n;
        if(expr->saved)return;

        data->expr_updated=1;
        expr->saved=1;

        Y_PRINT1("saving prov_expr(%s)\n",y_str_ptr(expr->id));
        __y_save_node(data,expr->id,expr->info,data->exprF);

        foreach(lc,expr->from)
        {
            _y_save_node(data,lfirst(lc));
            _y_node_destroy(lfirst(lc));
        }
        list_free(expr->from);expr->from=0;
        Y_PRINT1("saved prov_expr(%s)\n",y_str_ptr(expr->id));
    }
    else if(y_prov_is(n,prov_cexpr))
    {
        y_prov_cexpr* expr=n;
        if(expr->saved)return;

        data->cexpr_updated=1;
        expr->saved=1;

        Y_PRINT1("saving prov_cexpr(%s)\n",y_str_ptr(expr->id));
        __y_save_node(data,expr->id,expr->info,data->cexprF);

        foreach(lc,expr->from)
        {
            _y_save_node(data,lfirst(lc));
            _y_node_destroy(lfirst(lc));
        }
        list_free(expr->from);expr->from=0;
        Y_PRINT1("saved prov_cexpr(%s)\n",y_str_ptr(expr->id));
    }
    else if(!y_prov_is(n,prov_column))
    {
        elog(ERROR,"attempt to save <?%d> node",*((int*)n));
    }
}

void y_doSave_tbl(y_prov_data _data,y_tNode _n)
{
    _y_save_node(_data,_n);
}

void y_removeRel_tbl(y_prov_data _data,const char* name)
{
    y_prov_data_t* data=_data;
    if(!name)
        return;
    elog(WARNING,"rm name=%s",name);
    cJSON_DeleteItemFromObject(data->relmapJ,name);
    char* buf=cJSON_Print(data->relmapJ);
    elog(WARNING,"%s",buf);
    data->relmap_updated=1;
}

/*
 * ---------------------------obj save end-------------------------------
 * */


/*
 * --------------------------prov col--------------------------------------
 * */

y_cNode y_cNode_copy(y_cNode n){ return y_shared_cpy(n);}

void y_cNode_destroy(y_cNode n){ _y_node_destroy(n);}


List* y_relColList_col(y_prov_data _data,y_tNode tn)
{
    int i;
    List* returns=0;
    y_prov_rel* rel=(y_prov_rel*)tn;
    int col_cnt;
    ListCell* lc;
    cJSON* colArr;
    cJSON* relName;

    if(rel->cols)
    {
        foreach(lc,rel->cols)
        {
            returns=lappend(returns,y_cNode_copy(lfirst(lc)));
        }
        return returns;
    }

    col_cnt=cJSON_GetObjectItem(rel->info,"cnt")->valueint;
    colArr=cJSON_GetObjectItem(rel->info,"cols");
    relName=cJSON_GetObjectItem(rel->info,"name");

    for(i=0;i<col_cnt;++i)
    {
        y_prov_column* column=_y_column_alloc();
        cJSON* colInfo=cJSON_GetArrayItem(colArr,i);
        const char* _name=cJSON_GetObjectItem(colInfo,"name")->valuestring;
        column->idx=i;
        _Y_STR(column->id,y_str_alloc_cpy,rel->id);
        _Y_STR(column->content,y_str_alloc_cpy_args,"ccc",relName->valuestring,".",_name);
        column->info=colInfo;
        column->from=0;

        returns=lappend(returns,column);
        rel->cols=lappend(rel->cols,y_cNode_copy(column));
    }
    return returns;
}

List* y_valuesList_col(y_prov_data _data,y_tNode tn)
{
    /*FIX ME when y_prov_rel is different with y_tprov_rel*/
    return y_relColList_col(_data,tn);
}

List* y_func_col(y_prov_data _data,y_tNode tn)
{
    int i;
    List* returns=0;
    y_prov_trel* func=(y_prov_trel*)tn;
    int col_cnt;
    ListCell* lc;
    cJSON* returnsArr;
    cJSON* funcName;

    if(func->cols)
    {
        foreach(lc,func->cols)
        {
            returns=lappend(returns,y_cNode_copy(lfirst(lc)));
        }
        return returns;
    }

    col_cnt=cJSON_GetObjectItem(func->info,"rcnt")->valueint;
    returnsArr=cJSON_GetObjectItem(func->info,"returns");
    funcName=cJSON_GetObjectItem(func->info,"name");

    for(i=0;i<col_cnt;++i)
    {
        y_prov_column* column=_y_column_alloc();
        cJSON* returnInfo=cJSON_GetArrayItem(returnsArr,i);
        const char* _name=cJSON_GetObjectItem(returnInfo,"name")->valuestring;
        column->idx=i;
        _Y_STR(column->id,y_str_alloc_cpy,func->id);
        _Y_STR(column->content,y_str_alloc_cpy_args,"ccc",funcName->valuestring,".",_name);
        column->info=returnInfo;
        column->from=0;
        returns=lappend(returns,column);
        func->cols=lappend(func->cols,y_cNode_copy(column));
    }
    return returns;
}

static char _y_is_list_member_ptr(List* list,void* p)
{
    ListCell* lc;
    foreach(lc,list)
    {
        if(lfirst(lc)==p)
            return 1;
    }
    return 0;
}

static List* _y_lappend_unique(List* froms,y_cNode cn)
{
    if(y_prov_is(cn,prov_column) || y_prov_is(cn,prov_cexpr))
    {
        if(!_y_is_list_member_ptr(froms,cn))
            froms=lappend(froms,y_cNode_copy(cn));
    }
    else
    {
        Assert(y_prov_is(cn,prov_cexpr_tmp));
        y_prov_cexpr_tmp* ce=cn;
        ListCell* lc;
        List* _add=0;
        foreach(lc,ce->from)
        {
            if(!_y_is_list_member_ptr(froms,cn))
                _add=lappend(_add,y_cNode_copy(cn));
        }
        froms=list_concat(froms,_add);
        list_free(_add);
    }
    return froms;
}

extern char y_data_type_category(Oid o);
extern const char* y_const_to_str(Const* con);
y_cNode y_expr_col(y_prov_data data,NodeTag tag,void* exprInfo,List* _args/*list of y_cNode*/)
{
    ListCell* lc;
    y_prov_cexpr_tmp* cexpr;
    char is_com=0;
    y_str* content;
    List* args=0;
    List* froms=0;

    switch (tag)
    {
        case T_OpExpr:
        {
            y_op* op=exprInfo;
            y_cNode cn1,cn2;
            is_com=1;
            args=_args;
            if(op->kind=='l')
            {
                cn1=linitial(args);
                _Y_STR(content,y_str_alloc_cpy_args,"yc",y_cExprInfo(cn1),op->name);
            }
            else if(op->kind=='r')
            {
                cn1=linitial(args);
                _Y_STR(content,y_str_alloc_cpy_args,"cy",op->name,y_cExprInfo(cn1));
            }
            else
            {
                Assert(op->kind=='b');
                cn1=linitial(args);
                cn2=lsecond(args);
                _Y_STR(content,y_str_alloc_cpy_args,"ycy",y_cExprInfo(cn1),op->name,y_cExprInfo(cn2));
            }
            break;
        }
        case T_BoolExpr:
        {
            BoolExpr* boolExpr=exprInfo;
            is_com=1;
            args=_args;
            if(boolExpr->boolop==AND_EXPR)
            {
                y_cNode cn1=linitial(args);
                y_cNode cn2=lsecond(args);
                _Y_STR(content,y_str_alloc_cpy_args,"ycccy",
                                             y_cExprInfo(cn1),
                                             " ",Y_SYMX_STR(and)," ",
                                             y_cExprInfo(cn2));
            }
            else if(boolExpr->boolop==OR_EXPR)
            {
                y_cNode cn1=linitial(args);
                y_cNode cn2=lsecond(args);
                _Y_STR(content,y_str_alloc_cpy_args,"ycccy",
                                             y_cExprInfo(cn1),
                                             " ",Y_SYMX_STR(or)," ",
                                             y_cExprInfo(cn2));
            }
            else/*NOT_EXPR*/
            {
                y_cNode cn1=linitial(args);
                _Y_STR(content,y_str_alloc_cpy_args,"ccy",
                                             Y_SYMX_STR(or)," ",
                                             y_cExprInfo(cn1));
            }
            break;
        }
        case T_FuncExpr:
        {
            y_proc* proc=exprInfo;
            y_prov_data_t* data_=data;
            const char* argsStr;

            args=_args;
            y_strBuf_reset(data_->strBuf);
            y_cExprInfoList(&data_->strBuf,args);
            argsStr=y_strBuf_buf(data_->strBuf);

            if(proc->namespace)
                _Y_STR(content,y_str_alloc_cpy_args,"cccccc",proc->namespace,".",proc->name,
                                             "(",argsStr,")")
            else
                _Y_STR(content,y_str_alloc_cpy_args,"cccc",proc->name,
                                             "(",argsStr,")")
            break;
        }
        case T_Const:
        {
            Const* con=exprInfo;
            const char* value=y_const_to_str(con);

            if(y_data_type_category(con->consttype)==TYPCATEGORY_STRING)//FIX ME
            {
                _Y_STR(content,y_str_alloc_cpy_args,"cyc","'",value,"'");
            }
            else
            {
                _Y_STR(content,y_str_alloc_cpy_c,value);
            }

            break;
        }
        /*FIX ME:should be able to handle more case of expr types*/
        default:
        {
            _Y_STR(content,y_str_alloc_cpy_c,"<expr?>");
            elog(WARNING,"unknown expr in y_cExpr_col");
            return _y_cexpr_tmp_alloc(content,is_com);
        }
    }

    foreach(lc,args)
    {
        y_cNode cn=lfirst(lc);
        froms=_y_lappend_unique(froms,cn);
    }

    cexpr=_y_cexpr_tmp_alloc(content,is_com);
    cexpr->from=froms;

    return cexpr;
}

y_cNode y_setOp_col(y_prov_data _data,SetOperation setOp,
                    y_cNode n1,y_cNode n2)
{
    y_str* content;
    y_prov_cexpr_tmp* cexpr;
    const char* opStr="<setOp?>";

    if(setOp==SETOP_UNION)
        opStr=Y_SYMX_STR(union);
    else if(setOp==SETOP_INTERSECT)
        opStr=Y_SYMX_STR(intersect);
    else if(setOp==SETOP_EXCEPT)
        opStr=Y_SYMX_STR(except);

    _Y_STR(content,y_str_alloc_cpy_args,"ycccy",y_cExprInfo(n1),
                                 " ",opStr," ",
                                 y_cExprInfo(n2));

    cexpr=_y_cexpr_tmp_alloc(content,1);
    cexpr->from=_y_lappend_unique(cexpr->from,n1);
    cexpr->from=_y_lappend_unique(cexpr->from,n2);

    return cexpr;
}

static void _y_fromTo_col(y_cNode fn,y_cNode tn)
{
    cJSON* fromJ=cJSON_CreateObject();
    if(y_prov_is(fn,prov_cexpr))
    {
        y_prov_cexpr* fexpr=(y_prov_cexpr*)fn;
        cJSON_AddStringToObject(fromJ,"id",y_str_ptr(fexpr->id));
    }
    else
    {
        Assert(y_prov_is(fn,prov_column));
        y_prov_column* fcolumn=(y_prov_column*)fn;
        cJSON_AddStringToObject(fromJ,"id",y_str_ptr(fcolumn->id));
        cJSON_AddNumberToObject(fromJ,"idx",fcolumn->idx);
    }

    if(y_prov_is(tn,prov_cexpr))
    {
        y_prov_cexpr* texpr=(y_prov_cexpr*)tn;
        cJSON* fromArrJ=cJSON_GetObjectItem(texpr->info,"from");
        Assert(fromArrJ);
        cJSON_AddItemToArray(fromArrJ,fromJ);
        texpr->from=lappend(texpr->from,y_shared_cpy(fn));
    }
    else
    {
        Assert(y_prov_is(tn,prov_column));
        y_prov_column* tcolumn=(y_prov_column*)tn;
        Assert(!cJSON_GetObjectItem(tcolumn->info,"from"));
        cJSON_AddItemToObject(tcolumn->info,"from",fromJ);
        tcolumn->from=lappend(tcolumn->from,y_shared_cpy(fn));
    }
}

void y_fromTo_col(y_prov_data _data,y_cNode fn,y_cNode tn)
{
    if(fn==tn)
        return;

    if(y_prov_is(fn,prov_cexpr_tmp))
    {
        _y_tmp_to_cexpr(_data,(y_prov_cexpr_tmp*)fn);
    }
    if(y_prov_is(tn,prov_cexpr_tmp))
    {
        _y_tmp_to_cexpr(_data,(y_prov_cexpr_tmp*)tn);
    }

    _y_fromTo_col(fn,tn);
}

/*
 * --------------------------prov col end--------------------------------------
 * */

/*
 * --------------------------prov tbl--------------------------------------
 * */

y_tNode y_tNode_copy(y_tNode n){ return y_shared_cpy(n);}

void y_tNode_destroy(y_tNode n){ _y_node_destroy(n);}

#define _Y_EXPR1_BEGIN  \
y_tNode n;   \
const char* e1;          \
const char* desc;   \
y_prov_data_t* data=_data

#define _Y_EXPR1_END    \
n=_y_expr_alloc(data,desc,e1,0); \
return n

y_tNode y_common_tbl(y_prov_data data,const char* str)
{
    return _y_expr_alloc(data,Y_SYMX_STR(common),str,0);
}

y_tNode y_setOp_tbl(y_prov_data data,SetOperation setOp)
{
    Assert(setOp!=SETOP_NONE);
    if(setOp==SETOP_UNION)
        return _y_expr_alloc(data,Y_SYMX_STR(union),0,0);
    if(setOp==SETOP_INTERSECT)
        return _y_expr_alloc(data,Y_SYMX_STR(intersect),0,0);
    /*setOp==SETOP_EXCEPT*/
        return _y_expr_alloc(data,Y_SYMX_STR(except),0,0);
}

y_tNode y_with_tbl(y_prov_data _data,CommonTableExpr* cte)
{
    y_prov_trel* n;
    y_prov_data_t* data=_data;
    ListCell* lc,*alc=0;
    const char* col;
    cJSON* colsJ;
    int col_cnt;
    int i;

    col_cnt=list_length(cte->ctecolnames);
    n=_y_trel_alloc(data,cte->ctename,Y_KIND_CTE_STR,col_cnt);

    colsJ=cJSON_CreateArray();

    if(cte->aliascolnames)
        alc=list_head(cte->aliascolnames);
    for(i=0,lc=list_head(cte->ctecolnames);lc!=NULL;++i,lc=lnext(lc))
    {
        if(alc)
        {
            col=strVal(lfirst(alc));
            cJSON_AddItemToArray(colsJ,_y_colInfo_alloc(col,0));
            alc=lnext(alc);
        }
        else
        {
            col=strVal(lfirst(lc));
            cJSON_AddItemToArray(colsJ,_y_colInfo_alloc(col,0));
        }
    }
    cJSON_AddItemToObject(n->info,"cols",colsJ);

    _Y_DEBUG_PRINT_INFO(n,prov_trel);

    return n;
}

y_tNode y_values_tbl(y_prov_data _data,Alias* alias,List* types)
{
    y_prov_trel* n;
    y_prov_data_t* data=_data;
    ListCell* lc,*nlc=0;
    cJSON* colsJ;
    int col_cnt;
    int i;
    const char* name=alias->aliasname;

    col_cnt=list_length(types);
    n=_y_trel_alloc(data,name,Y_KIND_VALUES_STR,col_cnt);

    colsJ=cJSON_CreateArray();
    if(alias->colnames)
        nlc=list_head(alias->colnames);
    for(i=0,lc=list_head(types);lc!=NULL;++i,lc=lnext(lc))
    {
        y_data_type* dt=lfirst(lc);
        if(nlc)
        {
            cJSON_AddItemToArray(colsJ,_y_colInfo_alloc(strVal(lfirst(nlc)),dt->name));
            nlc=lnext(nlc);
        }
        else
            cJSON_AddItemToArray(colsJ,_y_colInfo_alloc(0,dt->name));
    }
    cJSON_AddItemToObject(n->info,"cols",colsJ);

    _Y_DEBUG_PRINT_INFO(n,prov_trel);

    return n;
}

y_tNode y_unknown_tbl(y_prov_data data,const char* desc)
{
    y_str* str;
    _Y_STR(str,y_str_alloc_cpy_args,"ccc","<",desc,"?>");
    y_prov_node_t* n=y_common_tbl(data,y_str_ptr(str));
    y_str_destroy(str);
    return n;
}

y_tNode y_groupBy_tbl(y_prov_data _data,
                      List* gby_list/*list of y_cNode*/,
                      List* cols/*list of y_cNode*/)
{
    y_str* e1Str;
    const char *e1,*e2;
    y_tNode n;
    y_prov_data_t* data=_data;

    y_strBuf_reset(data->strBuf);
    y_cExprInfoList(&data->strBuf,gby_list);
    e1Str=y_get_str(data->strBuf);
    e1=y_str_ptr(e1Str);

    y_strBuf_reset(data->strBuf);
    y_cExprInfoList(&data->strBuf,cols);
    e2=y_strBuf_buf(data->strBuf);

    n=_y_expr_alloc(data,Y_SYMX_STR(alpha),e1,e2);
    y_str_destroy(e1Str);
    _Y_DEBUG_PRINT_INFO(n,prov_expr);
    return n;
}

y_tNode y_join_tbl(y_prov_data _data,JoinExpr* je,
                   List* usings/*list of y_cNode*/,y_cNode quals)
{
    const char* e1=0;
    y_prov_data_t* data=_data;
    JoinType jt=je->jointype;
    const char* desc=Y_SYMX_STR(uJoin);

    switch (jt)
    {
        case JOIN_INNER:
            if(je->isNatural)
                desc=Y_SYMX_STR(nJoin);
            else
                desc=Y_SYMX_STR(join);
            break;
        case JOIN_FULL:
            desc=Y_SYMX_STR(fJoin);
            break;
        case JOIN_LEFT:
            desc=Y_SYMX_STR(lJoin);
            break;
        case JOIN_RIGHT:
            desc=Y_SYMX_STR(rJoin);
            break;
        default:
            break;
    }

    y_strBuf_reset(data->strBuf);
    if(je->usingClause)
    {
        data->strBuf=y_strBuf_append(data->strBuf,"c","using(");
        y_cExprInfoList(&data->strBuf,usings);
        data->strBuf=y_strBuf_append(data->strBuf,"c",")");
        e1=y_strBuf_buf(data->strBuf);
    }
    else if(je->quals)
    {
        data->strBuf=y_strBuf_append(data->strBuf,"c","on(");
        data->strBuf=y_strBuf_append(data->strBuf,"y",y_cExprInfo(quals));
        data->strBuf=y_strBuf_append(data->strBuf,"c",")");
        e1=y_strBuf_buf(data->strBuf);
    }

    return _y_expr_alloc(data,desc,e1,0);
}

y_tNode y_func_tbl(y_prov_data _data,const char* name,List* args/*list of y_cNode*/,
                   int returns_cnt,const char** returns_name,y_data_type** returns_type)
{
    y_prov_trel* n;
    int i;
    ListCell* lc;
    int args_cnt;
    y_prov_data_t* data=_data;

    args_cnt=list_length(args);

    n=_y_func_alloc(data,name,args_cnt,returns_cnt);

    if(args_cnt>0)
    {
        cJSON* argsArr=cJSON_CreateArray();
        foreach(lc,args)
        {
            const char* arg=y_str_ptr(y_cExprInfo(lfirst(lc)));
            n->rangeNodes=_y_lappend_unique(n->rangeNodes,lfirst(lc));
            cJSON_AddItemToArray(argsArr,cJSON_CreateString(arg));
        }
        cJSON_AddItemToObject(n->info,"args",argsArr);
    }
    else
        cJSON_AddNullToObject(n->info,"args");

    if(returns_cnt>0)
    {
        cJSON* returnsArr=cJSON_CreateArray();
        for(i=0;i<returns_cnt;++i)
        {
            cJSON* returnJ;
            if(returns_type)
                returnJ=_y_colInfo_alloc(returns_name[i],returns_type[i]->name);
            else
                returnJ=_y_colInfo_alloc(returns_name[i],0);
            cJSON_AddItemToArray(returnsArr,returnJ);
        }
        cJSON_AddItemToObject(n->info,"returns",returnsArr);
    }
    else
        cJSON_AddNullToObject(n->info,"returns");

    _Y_DEBUG_PRINT_INFO(n,prov_trel);

    return n;
}

y_tNode y_project_tbl(y_prov_data _data,List* cols)
{
    _Y_EXPR1_BEGIN;

    desc=Y_SYMX_STR(pi);
    y_strBuf_reset(data->strBuf);
    y_cExprInfoList(&data->strBuf,cols);
    e1=y_strBuf_buf(data->strBuf);

    _Y_EXPR1_END;
}

y_tNode y_prod_tbl(y_prov_data data)
{
    return _y_expr_alloc(data,Y_SYMX_STR(prod),0,0);
}

y_tNode y_filter_tbl(y_prov_data _data,y_cNode whereExpr)
{
    _Y_EXPR1_BEGIN;
    desc=Y_SYMX_STR(sigma);
    e1=y_str_ptr(y_cExprInfo(whereExpr));
    _Y_EXPR1_END;
}

y_tNode y_rel_tbl(y_prov_data _data,y_relation_entry* re)
{
    y_tNode re_node=0;
    y_prov_data_t* data=_data;

    y_str* name;
    if(re->namespace)
    _Y_STR(name,y_str_alloc_cpy_args,"ccc",re->namespace,".",re->name)
    else
    _Y_STR(name,y_str_alloc_keep_cStr,re->name)

    re_node=_y_rel_get(data,y_str_ptr(name),re);

    if(!re_node)
    {
        int i;
        cJSON* colsJ;
        const char* kind=Y_KIND_REGULAR_TABLE_STR;
        if(re->kind=='t')
            kind=Y_KIND_TMP_TABLE_STR;
        else if(re->kind=='v')
            kind=Y_KIND_VIEW_STR;

        y_prov_rel* n=0;
        if(re->kind=='t')//FIX ME
            n=(y_prov_rel*)_y_trel_alloc(data,y_str_ptr(name),kind,re->col_cnt);
        else
            n=_y_rel_alloc(data,y_str_ptr(name),kind,re->col_cnt);

        colsJ=cJSON_CreateArray();
        for(i=0;i<re->col_cnt;++i)
        {
            cJSON_AddItemToArray(colsJ,
                                 _y_colInfo_alloc(re->col_entries[i].name,
                                                  re->col_entries[i].data_type->name));
        }
        cJSON_AddItemToObject(n->info,"cols",colsJ);

        re_node=n;
    }

    y_str_destroy(name);/*attention!*/


    _Y_DEBUG_PRINT_INFO(re_node,prov_rel);
    return re_node;
}

void y_fromTo_tbl(y_prov_data _data,y_tNode from,y_tNode to)
{
    if(from==to)
        return;

    y_str* fromId=y_get_prov_id(from);
    switch (((y_prov_node_t*)to)->type)
    {
        case yT_prov_expr:
        {
            y_prov_expr* expr=to;
            cJSON* fromArrJ=cJSON_GetObjectItem(expr->info,"from");
            cJSON* fromJ=cJSON_CreateString(y_str_ptr(fromId));
            cJSON_AddItemToArray(fromArrJ,fromJ);
            expr->from=lappend(expr->from,y_shared_cpy(from));
            break;
        }
        case yT_prov_rel:
        case yT_prov_trel:/*FIX ME*/
        {
            y_prov_rel* rel=to;
            cJSON* fromArrJ=cJSON_GetObjectItem(rel->info,"from");
            Assert(fromArrJ);
            cJSON* fromJ=cJSON_CreateString(y_str_ptr(fromId));
            cJSON_AddItemToArray(fromArrJ,fromJ);
            rel->rangeNodes=lappend(rel->rangeNodes,y_shared_cpy(from));
            break;
        }
        default:
            elog(ERROR,"link to a unknown tNode");
    }
}
