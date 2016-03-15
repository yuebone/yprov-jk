//
// Created by bon on 15-11-14.
//

#ifndef PG_PROV_Y_SAVE_H
#define PG_PROV_Y_SAVE_H

#include <postgres.h>
#include <fmgr.h>
#include <nodes/parsenodes.h>
#include "y_schemas.h"

typedef void* y_prov_data;
typedef void* y_tNode;
typedef void* y_cNode;

extern y_prov_data  y_prov_data_init(void* arg);
extern void         y_prov_data_saveAll(y_prov_data data);
extern void         y_prov_data_fini(y_prov_data data);

//-----------------tbl-------------------------

y_tNode y_tNode_copy(y_tNode n);
void    y_tNode_destroy(y_tNode n);

y_tNode y_rel_tbl(y_prov_data data,y_relation_entry* re);
y_tNode y_prod_tbl(y_prov_data data);
y_tNode y_project_tbl(y_prov_data data,List* cols/*list of y_cNode*/);
y_tNode y_filter_tbl(y_prov_data data,y_cNode whereExpr);
y_tNode y_groupBy_tbl(y_prov_data data,List* gby_list/*list of y_cNode*/,
                      List* cols/*list of y_cNode*/);
y_tNode y_unknown_tbl(y_prov_data data,const char* desc);
y_tNode y_func_tbl(y_prov_data data,const char* name,List* args/*list of y_cNode*/,
                   int returns_cnt,const char** returns_name,y_data_type** returns_type);
y_tNode y_join_tbl(y_prov_data data,JoinExpr* je,List* usings/*list of y_cNode*/,y_cNode quals);
y_tNode y_values_tbl(y_prov_data data,Alias* alias,List* types);
y_tNode y_with_tbl(y_prov_data data,CommonTableExpr* cte);
y_tNode y_setOp_tbl(y_prov_data data,SetOperation setOp);
y_tNode y_common_tbl(y_prov_data data,const char* str);
void    y_fromTo_tbl(y_prov_data data,y_tNode from,y_tNode to);
void    y_doSave_tbl(y_prov_data data,y_tNode n);
void    y_output_tbl(y_prov_data data,y_tNode n);/*mainly for debug*/
void    y_removeRel_tbl(y_prov_data data,const char* name);

//-----------------col-------------------------

y_cNode y_cNode_copy(y_cNode n);
void    y_cNode_destroy(y_cNode n);

List*   y_valuesList_col(y_prov_data data,y_tNode tn);
List*   y_relColList_col(y_prov_data data,y_tNode tn);
List*   y_func_col(y_prov_data data,y_tNode tn);

//FIX ME
y_cNode y_expr_col(y_prov_data data,NodeTag tag,void* exprInfo,List* args/*list of y_cNode*/);

y_cNode y_setOp_col(y_prov_data data,SetOperation setOp,
                     y_cNode n1,y_cNode n2);

void    y_fromTo_col(y_prov_data data,y_cNode from,y_cNode to);

#endif //PG_PROV_Y_SAVE_H
