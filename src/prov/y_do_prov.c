//
// Created by bon on 15-6-16.
//
#include <postgres.h>
#include <access/heapam.h>
#include <utils/rel.h>
#include "y_save.h"
#include "y_prov.h"
#include "utils/y_io.h"

typedef struct
{
    y_tNode tbl;
    int col_cnt;
    y_cNode* col_nodes;//array of y_cNode
}y_rtable;

typedef struct
{
    const char* name;//name of cte
    y_rtable tbl;
}y_cte;

typedef struct y_status_t
{
    struct y_status_t* parent;

    y_prov* prov;
    y_prov_data data;

    Alias* alias_of_q;
    Query* q;//query tree

    int cte_cnt;
    y_cte* ctes;

    int rtable_cnt;
    y_rtable* rtables;

    int lvl;//level
}y_status;

static void y_transformQuery(y_status* st,y_rtable* returnTbl);
static y_cNode y_transformTargetExpr(y_status* st,Expr* qe);

/*free a list and contents of list cells*/
static void _y_nodesList_destroy(List* l)
{
    if(!l)
        return;
    ListCell* lc;
    foreach(lc,l)
    {
        y_cNode n=lfirst(lc);
        y_cNode_destroy(n);
    }
    list_free(l);
}

static void _y_rtable_init(y_rtable* rt,int cnt)
{
    rt->col_cnt=cnt;
    if(cnt<=0)
        return;
    rt->col_nodes=palloc(sizeof(y_cNode)*cnt);
}

/*init des and copy content from src*/
static void _y_rtable_init_cpy(y_rtable* des,y_rtable* src)
{
    int i;
    if(des==src)
        return;
    _y_rtable_init(des,src->col_cnt);
    des->tbl=y_tNode_copy(src->tbl);
    for(i=0;i<des->col_cnt;++i)
        des->col_nodes[i]=y_cNode_copy(src->col_nodes[i]);
}

static void _y_rtable_fini(y_rtable* rt)
{
    if(rt->col_cnt<=0)
        return;

    if(rt->col_cnt>0)
    {
        int c;
        for(c=0;c<rt->col_cnt;++c)
        {
            y_cNode_destroy(rt->col_nodes[c]);
        }
        pfree(rt->col_nodes);
    }

    rt->col_cnt=0;
}

static void _y_status_init(y_status* st,
                           y_prov* prov,y_prov_data data,
                           Query* q,int cte_cnt,
                           int rtable_cnt)
{
    st->parent=NULL;
    st->prov=prov;
    st->data=data;
    st->q=q;
    st->alias_of_q=0;

    st->cte_cnt=cte_cnt;
    if(cte_cnt>0)
    {
        st->ctes=palloc0(cte_cnt*sizeof(y_cte));
    }

    st->rtable_cnt=rtable_cnt;
    if(rtable_cnt>0)
    {
        st->rtables=palloc0(rtable_cnt*(sizeof(y_rtable)));
    }

    st->lvl=1;
}

inline void _y_status_init2(y_status* st,y_status* parent, Query* q)
{
    _y_status_init(st,parent->prov,parent->data,q,
                   list_length(q->cteList),
                   list_length(q->rtable));
    st->parent=parent;
    st->lvl=parent->lvl+1;
}

static void _y_status_fini(y_status* st)
{
    int i;
    for(i=0;i<st->rtable_cnt;++i)
        _y_rtable_fini(st->rtables+i);
    if(st->rtable_cnt>0)
        pfree(st->rtables);

    if(st->cte_cnt>0)
    {
        for(i=0;i<st->cte_cnt;++i)
            _y_rtable_fini(&st->ctes[i].tbl);
        pfree(st->ctes);
    }
}

static y_cNode y_transformOpExpr(y_status* st,OpExpr* op)
{
    y_cNode cn;
    Expr* arg1=0;
    Expr* arg2=0;
    List* c_args=0;
    y_op* yOp=y_op_byOid(st->prov,op->opno);

    if(yOp->kind=='b')
    {
        arg1=linitial(op->args);
        arg2=lsecond(op->args);
    }
    else
        arg1=linitial(op->args);

    if(arg1)
    {
        y_cNode cn1=y_transformTargetExpr(st,arg1);
        c_args=lappend(c_args,cn1);
    }
    if(arg2)
    {
        y_cNode cn2=y_transformTargetExpr(st,arg2);
        c_args=lappend(c_args,cn2);
    }

    cn=y_expr_col(st->data,nodeTag(op),yOp,c_args);

    _y_nodesList_destroy(c_args);
    return cn;
}

static y_cNode y_transformBoolExpr(y_status* st,BoolExpr* be)
{
    y_cNode cn;
    Expr* arg1=0;
    Expr* arg2=0;
    List* c_args=0;

    if(be->boolop!=NOT_EXPR)
    {
        arg1=linitial(be->args);
        arg2=lsecond(be->args);
    }
    else
        arg1=linitial(be->args);
    if(arg1)
    {
        y_cNode cn1=y_transformTargetExpr(st,arg1);
        c_args=lappend(c_args,cn1);
    }
    if(arg2)
    {
        y_cNode cn2=y_transformTargetExpr(st,arg2);
        c_args=lappend(c_args,cn2);
    }

    cn=y_expr_col(st->data,nodeTag(be),be,c_args);

    _y_nodesList_destroy(c_args);
    return cn;
}

//func as column target
static y_cNode y_transformFuncC(y_status* st,Expr* qe)
{
    y_cNode cn;
    List *args,*c_args=0;
    ListCell* lc;
    Oid oid;
    y_proc* proc;
    char arg_is_star=0;

    if(IsA(qe,Aggref))
    {
        args=((Aggref*)qe)->args;
        oid=((Aggref*)qe)->aggfnoid;
        arg_is_star=((Aggref*)qe)->aggstar;
    }
    else
    {
        args=((FuncExpr*)qe)->args;
        oid=((FuncExpr*)qe)->funcid;
    }

    proc=y_proc_byOid(st->prov,oid);

    if(arg_is_star)
    {
        y_cNode arg_cNode=y_expr_col(st->data,T_Const,"*",0);
        c_args=lappend(c_args,arg_cNode);
    }
    else
    {
        foreach(lc,args)
        {
            y_cNode arg_cNode;
            Expr* arg=lfirst(lc);

            arg_cNode=y_transformTargetExpr(st,arg);
            c_args=lappend(c_args,arg_cNode);
        }
    }

    cn=y_expr_col(st->data,T_FuncExpr,proc,c_args);
    _y_nodesList_destroy(c_args);

    return cn;
}

static inline y_cNode y_transformVar(y_status* st,Var* var)
{
    y_cNode cn;
    y_cNode* rel_nodes=st->rtables[var->varno-1].col_nodes;
    cn=y_cNode_copy(rel_nodes[var->varattno-1]);

    return cn;
}

static inline y_cNode y_transformConst(y_status* st,Const* con)
{
    return y_expr_col(st->data,nodeTag(con),con,0);
}

static y_cNode y_transformTargetExpr(y_status* st,Expr* qe)
{
    switch (nodeTag(qe))
    {
        case T_Var:
        Y_PRINT0("case T_Var\n");
            return y_transformVar(st,(Var*)qe);
        case T_Const:
        Y_PRINT0("case T_Const\n");
            return y_transformConst(st,(Const*)qe);
        case T_BoolExpr:
        Y_PRINT0("case T_BoolExpr\n");
            return y_transformBoolExpr(st,(BoolExpr*)qe);
        case T_OpExpr:
        Y_PRINT0("case T_OpExpr\n");
            return y_transformOpExpr(st,(OpExpr*)qe);
        case T_Aggref:
        case T_FuncExpr:
        Y_PRINT0("case T_FuncExpr\n");
            return y_transformFuncC(st,qe);
        case T_TargetEntry:
            return y_transformTargetExpr(st,((TargetEntry*)qe)->expr);

        case T_RelabelType://FIX ME
        {
            RelabelType* relabelType=(RelabelType*)qe;
            return y_transformTargetExpr(st,relabelType->arg);
        }
        default:
        Y_PRINT1("case <%d?>\n",nodeTag(qe));
            return y_expr_col(st->data,nodeTag(qe),0,0);
    }
}

static y_rtable* y_transformRtable(y_status* st,Node* from,char* flags)
{
    if (IsA(from, RangeTblRef))
    {
        RangeTblRef* rtr=(RangeTblRef*)from;
        int i=rtr->rtindex-1;

        if(flags[i])
            return &st->rtables[i];

        RangeTblEntry* rte=list_nth(st->q->rtable,i);

        if(rte->rtekind==RTE_CTE)
        {
            Y_PRINT0("*from* RTE_CTE\n");
            int c;
            y_rtable* cte_tbl=0;
            int cte_lvl=rte->ctelevelsup;
            y_status* cte_in_st=st;

            while (cte_lvl>0)
            {
                --cte_lvl;
                cte_in_st=cte_in_st->parent;
            }

            for(c=0;c<cte_in_st->cte_cnt;++c)
            {
                if(strcmp(rte->ctename,cte_in_st->ctes[c].name)==0)
                {
                    cte_tbl=&cte_in_st->ctes[c].tbl;
                }
            }

            Assert(cte_tbl);
            _y_rtable_init_cpy(&st->rtables[i],cte_tbl);
            flags[i]=1;
            return st->rtables+i;
        }
        else if(rte->rtekind==RTE_RELATION)
        {
            Y_PRINT0("*from* RTE_RELATION\n");
            y_relation_entry* re=y_relation_byOid(st->prov,rte->relid);
            _y_rtable_init(&st->rtables[i],re->col_cnt);

            y_tNode tn=y_rel_tbl(st->data,re);
            List* col_nodes=y_relColList_col(st->data,tn);
            ListCell* clc;
            int c;

            st->rtables[i].tbl=tn;
            for (c=0,clc=list_head(col_nodes);c<re->col_cnt;++c,clc=lnext(clc))
            {
                y_cNode cn=lfirst(clc);
                st->rtables[i].col_nodes[c]=y_cNode_copy(cn);
            }

            _y_nodesList_destroy(col_nodes);
            flags[i]=1;
            return st->rtables+i;
        }
        else if(rte->rtekind==RTE_FUNCTION)
        {
            Y_PRINT0("*from* RTE_FUNCTION\n");
            //FIX ME:rte->functions may be list of more than 2 functions
            RangeTblFunction* rfunc=linitial(rte->functions);
            FuncExpr* fe=(FuncExpr*)(rfunc)->funcexpr;

            //FIX ME
            if(list_length(rte->functions)>1)
                elog(WARNING,"multiple functions as from-item,using first only");
            Assert(IsA(fe,FuncExpr));
            Assert(rfunc->funccolcount>0);

            int c;
            y_tNode tn;
            ListCell *qlc,*lc;
            List* arg_nodes=NULL;
            List* return_nodes=NULL;
            y_proc* proc=y_proc_byOid(st->prov,fe->funcid);
            y_str* name;
            List* returns_name_raw=0;
            const char** returns_name=palloc(sizeof(const char*)*rfunc->funccolcount);
            //y_data_type** returns_type=palloc(sizeof(y_data_type*)*rfunc->funccolcount);

            if(proc->namespace)
                name=y_str_alloc_cpy_args("ccc",proc->namespace,".",proc->name);
            else
                name=y_str_alloc_keep_cStr(proc->name);

            //handle args
            ++st->lvl;
            foreach(qlc,fe->args)
            {
                y_cNode arg_node=y_transformTargetExpr(st,lfirst(qlc));
                arg_nodes=lappend(arg_nodes,arg_node);
            }
            --st->lvl;

            //handle returns
            if(rte->alias && rte->alias->colnames)
                returns_name_raw=rte->alias->colnames;
            else if(rte->eref->colnames)
                returns_name_raw=rte->eref->colnames;
            else if(rfunc->funccolnames)
                returns_name_raw=rfunc->funccolnames;

            if(returns_name_raw)
            {
                for(c=0,lc=list_head(returns_name_raw);c<rfunc->funccolcount;++c,lc=lnext(lc))
                {
                    returns_name[c]=strVal(lfirst(lc));
                }
            }
            else
            {
                char colNameStr[32]={};
                for(c=0;c<rfunc->funccolcount;++c)
                {
                    int len=snprintf(colNameStr,32,"return%d",c+1);
                    char* strPlace=palloc(len+1);//it's best to be freed  later
                    memcpy(strPlace,colNameStr,len+1);
                    returns_name[c]=strPlace;
                }
            }

            tn=y_func_tbl(st->data,y_str_ptr(name),arg_nodes,
                          rfunc->funccolcount,returns_name,0/*FIX ME*/);
            return_nodes=y_func_col(st->data,tn);

            if(!returns_name_raw)
                for(c=0;c<rfunc->funccolcount;++c)
                    pfree((void*)returns_name[c]);//free strPlace

            _y_rtable_init(&st->rtables[i],list_length(return_nodes));
            for(c=0,lc=list_head(return_nodes);lc!=NULL;++c,lc=lnext(lc))
            {
                y_cNode return_node=lfirst(lc);
                st->rtables[i].col_nodes[c]=y_cNode_copy(return_node);
            }
            st->rtables[i].tbl=tn;

            pfree(returns_name);
            //pfree(returns_type);
            y_str_destroy(name);
            _y_nodesList_destroy(arg_nodes);
            _y_nodesList_destroy(return_nodes);
            flags[i]=1;
            return st->rtables+i;
        }
        else if(rte->rtekind==RTE_SUBQUERY)
        {
            Y_PRINT0("*from* RTE_SUBQUERY\n");
            Query *sub_q = rte->subquery;

            //go to subQuery
            y_status sub_s;
            _y_status_init2(&sub_s,st,sub_q);
            sub_s.alias_of_q=rte->alias;
            y_transformQuery(&sub_s,st->rtables+i);

            //back to curr
            _y_status_fini(&sub_s);

            flags[i]=1;
            return st->rtables+i;
        }
        else if(rte->rtekind==RTE_VALUES)
        {
            List* data_types=0;
            ListCell* lc;
            List* constValues=linitial(rte->values_lists);
            List* cNodes;
            y_tNode tn;
            int c;

            foreach(lc,constValues)
            {
                Const* con=lfirst(lc);
                y_data_type* data_type=y_data_type_byOid(st->prov,con->consttype);
                data_types=lappend(data_types,data_type);
            }

            if(st->alias_of_q)
                tn=y_values_tbl(st->data,st->alias_of_q,data_types);
            else
                tn=y_values_tbl(st->data,rte->eref,data_types);
            list_free(data_types);

            cNodes=y_valuesList_col(st->data,tn);

            _y_rtable_init(st->rtables+i,list_length(cNodes));
            st->rtables[i].tbl=tn;
            c=0;foreach(lc,cNodes)
            {
                st->rtables[i].col_nodes[c]=y_cNode_copy((y_cNode)lfirst(lc));
                ++c;
            }

            flags[i]=1;
            return st->rtables+i;
        }
    }
    else if (IsA(from, JoinExpr))
    {
        Y_PRINT0("*from* JoinExpr\n");
        JoinExpr* je=(JoinExpr*)from;
        int i=je->rtindex-1;
        y_rtable *tbl_left,*tbl_right;
        int leftIdx,rightIdx;
        int leftCol_cnt,rightCol_cnt;
        y_tNode tn;
        List* using_cns=0;
        y_cNode quals_cn=0;
        ListCell* lc;
        int c;

        if(flags[i])
            return st->rtables+i;

        tbl_left=y_transformRtable(st,je->larg,flags);
        tbl_right=y_transformRtable(st,je->rarg,flags);
        leftCol_cnt=tbl_left->col_cnt;
        rightCol_cnt=tbl_right->col_cnt;

        leftIdx=(nodeTag(je->larg)==T_JoinExpr)?
                (((JoinExpr*)je->larg)->rtindex)-1:
                (((RangeTblRef*)je->larg)->rtindex)-1;
        rightIdx=(nodeTag(je->rarg)==T_JoinExpr)?
                 (((JoinExpr*)je->rarg)->rtindex)-1:
                 (((RangeTblRef*)je->rarg)->rtindex)-1;

        _y_rtable_init(&st->rtables[i],leftCol_cnt+rightCol_cnt);
        for(c=0;c<leftCol_cnt;++c)
        {
            y_cNode cn=st->rtables[leftIdx].col_nodes[c];
            st->rtables[i].col_nodes[c]=y_cNode_copy(cn);
        }
        for(c=0;c<rightCol_cnt;++c)
        {
            y_cNode cn=st->rtables[rightIdx].col_nodes[c];
            st->rtables[i].col_nodes[c+leftCol_cnt]=y_cNode_copy(cn);
        }

        if(je->usingClause)
        {
            foreach(lc,je->usingClause)
            {
                y_cNode cn=y_transformTargetExpr(st,lfirst(lc));
                using_cns=lappend(using_cns,cn);
            }
        }
        if(je->quals)
        {
            Expr* expr=(Expr*)je->quals;
            quals_cn=y_transformTargetExpr(st,expr);
        }

        tn=y_join_tbl(st->data,je,using_cns,quals_cn);
        y_fromTo_tbl(st->data,st->rtables[leftIdx].tbl,tn);
        y_fromTo_tbl(st->data,st->rtables[rightIdx].tbl,tn);
        st->rtables[i].tbl=tn;

        if(quals_cn)
            y_cNode_destroy(quals_cn);
        if(using_cns)
            _y_nodesList_destroy(using_cns);

        flags[i]=1;
        return st->rtables+i;
    }
    else //FIX ME
    {
        elog(ERROR,"unknown from item");
    }

    return 0;
}

static y_tNode y_transformFromList(y_status* st)
{
    ListCell* lc;
    char* rtable_flags=palloc0(st->rtable_cnt);
    y_tNode tn=0;
    y_tNode where_tn=0;

    Query* q=st->q;
    List* fromlist=q->jointree->fromlist;
    int from_cnt=list_length(fromlist);

    if(from_cnt>1)
    {
        /*prov tbl*/
        tn=y_prod_tbl(st->data);
        foreach(lc,fromlist)
        {
            Node* from=lfirst(lc);
            y_rtable* rtable=y_transformRtable(st,from,rtable_flags);
            /*prov tbl transition*/
            y_fromTo_tbl(st->data,rtable->tbl,tn);
        }
    }
    else if(from_cnt==1)
    {
        Node* from=linitial(fromlist);
        y_rtable* rtable=y_transformRtable(st,from,rtable_flags);
        tn=rtable->tbl;
    }

    pfree(rtable_flags);

    /*handling where*/
    if(st->q->jointree->quals)
    {
        //FIX ME
        y_cNode cn=y_transformTargetExpr(st,(Expr*)st->q->jointree->quals);
        where_tn=y_filter_tbl(st->data,cn);
        y_cNode_destroy(cn);
        Assert(tn);
        y_fromTo_tbl(st->data,tn,where_tn);
        y_tNode_destroy(tn);
        return where_tn;
    }

    return tn;
}

static void y_transformCTEs(y_status* st)
{
    int i;
    ListCell *lc;
    Y_PRINT0("y_transformCTEs\n");
    for(i=0,lc=list_head(st->q->cteList);
        lc!=NULL;++i,lc=lnext(lc))
    {
        y_rtable selectRel;
        y_status sub_s;
        Query* sub_q;
        y_tNode cteTn;
        List* cteCns;
        int c;
        ListCell* _lc;
        CommonTableExpr* cte=lfirst(lc);

        sub_q=(Query*)cte->ctequery;

        _y_status_init2(&sub_s,st,sub_q);

        st->ctes[i].name=cte->ctename;
        y_transformQuery(&sub_s,&selectRel);

        cteTn=y_with_tbl(st->data,cte);
        cteCns=y_relColList_col(st->data,cteTn);

        _y_rtable_init(&(st->ctes[i].tbl),selectRel.col_cnt);
        st->ctes[i].tbl.tbl=cteTn;
        for(_lc=list_head(cteCns),c=0;c<selectRel.col_cnt;++c,_lc=lnext(_lc))
        {
            y_cNode cteCn=lfirst(_lc);
            st->ctes[i].tbl.col_nodes[c]=cteCn;
            y_fromTo_col(st->data,selectRel.col_nodes[c],cteCn);
        }
        y_fromTo_tbl(st->data,selectRel.tbl,st->ctes[i].tbl.tbl);

        _y_rtable_fini(&selectRel);
        _y_status_fini(&sub_s);
    }
}


static void _y_transformQuery(y_status* st,y_rtable* returnTbl)
{
    ListCell* lc;

    y_tNode from_tn=0;
    y_tNode selectList_tn=0;/*for project or groupBy*/
    y_tNode having_tn=0;
    y_tNode top_tn=0,bottom_tn=0;

    int selectCols_cnt=0;
    List* target_cNodes=0;
    List* gby_cNodes=0;

    int i;

    Y_PRINT0("_transformQuery\n");

    /*handline ctes*/
    if(st->cte_cnt>0)
        y_transformCTEs(st);

    /*handling from list and where-item*/
    from_tn=y_transformFromList(st);

    foreach(lc,st->q->targetList)
    {
        TargetEntry* te=(TargetEntry*)lfirst(lc);
        if(!te->resjunk)
            ++selectCols_cnt;
    }

    _y_rtable_init(returnTbl,selectCols_cnt);

    /*handling target list*/
    /*prov col*/
    i=0;foreach(lc,st->q->targetList)
    {
        TargetEntry* te=(TargetEntry*)lfirst(lc);
        y_cNode cn=y_transformTargetExpr(st,te->expr);
        if(te->ressortgroupref>0)
        {
            if(!te->resjunk)
                cn=y_cNode_copy(cn);
            gby_cNodes=lappend(gby_cNodes,cn);
        }
        if(!te->resjunk)
        {
            returnTbl->col_nodes[i++]=cn;
            target_cNodes=lappend(target_cNodes,cn);
        }
    }

    /*handling group by*/
    Assert(target_cNodes);
    if(gby_cNodes)
    {
        selectList_tn=y_groupBy_tbl(st->data,gby_cNodes,target_cNodes);
    }
    else
    {
        selectList_tn=y_project_tbl(st->data,target_cNodes);
    }
    top_tn=selectList_tn;
    bottom_tn=top_tn;

    /*handling having*/
    if(st->q->havingQual)
    {
        //FIX ME
        y_cNode cn=y_transformTargetExpr(st,(Expr*)st->q->jointree->quals);
        having_tn=y_filter_tbl(st->data,cn);
        y_cNode_destroy(cn);

        y_fromTo_tbl(st->data,having_tn,bottom_tn);
        bottom_tn=having_tn;
    }

    if(from_tn)
    {
        y_fromTo_tbl(st->data,from_tn,bottom_tn);
    }

    returnTbl->tbl=y_tNode_copy(top_tn);

    if(selectList_tn)
        y_tNode_destroy(selectList_tn);
    if(having_tn)
        y_tNode_destroy(having_tn);
    if(from_tn)
        y_tNode_destroy(from_tn);

    _y_nodesList_destroy(gby_cNodes);
    list_free(target_cNodes);
}

static void y_transformSetOperation(y_status* st,y_rtable* returnTbl)
{
    Query* curr_q=st->q;

    y_status s_left;
    y_status s_right;
    y_rtable tbl_left;
    y_rtable tbl_right;
    Query* q_left;
    Query* q_right;

    y_tNode setN;

    SetOperationStmt* sos=(SetOperationStmt*)curr_q->setOperations;
    RangeTblRef* rtf;
    RangeTblEntry* rte_left,*rte_right;

    int col_cnt;
    int i;

    Y_PRINT0("transformSetOperation\n");

    if(st->cte_cnt>0)
        y_transformCTEs(st);

    SetOperation op=sos->op;

    rtf=(RangeTblRef*)sos->larg;
    rte_left=(RangeTblEntry*)list_nth(curr_q->rtable,rtf->rtindex-1);
    q_left=rte_left->subquery;

    rtf=(RangeTblRef*)sos->rarg;
    rte_right=(RangeTblEntry*)list_nth(curr_q->rtable,rtf->rtindex-1);
    q_right=rte_right->subquery;

    //go to left child
    _y_status_init2(&s_left,st,q_left);
    y_transformQuery(&s_left,&tbl_left);

    //go to right child
    _y_status_init2(&s_right,st,q_right);
    y_transformQuery(&s_right,&tbl_right);

    //back
    Assert(tbl_left.col_cnt==tbl_right.col_cnt);
    col_cnt=tbl_left.col_cnt;
    _y_rtable_init(returnTbl,col_cnt);

    /*prov tbl transition*/
    setN=y_setOp_tbl(st->data,op);
    y_fromTo_tbl(st->data,tbl_left.tbl,setN);
    y_fromTo_tbl(st->data,tbl_right.tbl,setN);
    returnTbl->tbl=setN;

    /*prov col*/
    for(i=0;i<col_cnt;++i)
    {
        y_cNode cSetN=y_setOp_col(st->data,op,
                                   tbl_left.col_nodes[i], tbl_right.col_nodes[i]);

        returnTbl->col_nodes[i]=cSetN;
    }

    _y_rtable_fini(&tbl_left);
    _y_rtable_fini(&tbl_right);
    _y_status_fini(&s_left);
    _y_status_fini(&s_right);
}

static void y_transformQuery(y_status* st,y_rtable* returnTbl)
{
    Query* q=st->q;

    if(!q->setOperations)
        _y_transformQuery(st,returnTbl);
    else
        y_transformSetOperation(st,returnTbl);
}

static void y_transformUtilityStmt(y_prov* prov,y_prov_data d,Query* q)
{
    RangeVar* rv;
    y_relation_entry* re;
    const char* create_desc="";

    Y_PRINT0("transformUtilityStmt\n");

    if(IsA(q->utilityStmt,ViewStmt))
    {
        ViewStmt* qv=(ViewStmt*)(q->utilityStmt);
        rv=qv->view;
        q=(Query*)(qv->query);
        create_desc="CREATE VIEW AS";
    }
    else if(IsA(q->utilityStmt,CreateTableAsStmt))
    {
        CreateTableAsStmt* qc=(CreateTableAsStmt*)(q->utilityStmt);
        q=(Query*)(qc->query);
        rv=qc->into->rel;
        if(qc->is_select_into)
            create_desc="SELECT INTO";
        else
            create_desc="CREATE TABLE AS";
    }
    else
        return;

    /*
     * FIX ME
     * get relation oid by its name
     * */
    Relation rel = heap_openrv_extended(rv, AccessShareLock, 1);
    Oid rel_oid=RelationGetRelid(rel);
    heap_close(rel, NoLock);

    re=y_relation_byOid(prov,rel_oid);

    /*prov tbl*/
    y_tNode tn=y_rel_tbl(d,re);
    y_tNode tn2=y_common_tbl(d,create_desc);

    /*prov col*/
    List* cns=y_relColList_col(d,tn);

    //go to child query
    y_status status;
    y_rtable selectRel;
    _y_status_init(&status,prov,d,q,
                   list_length(q->cteList),
                   list_length(q->rtable));

    y_transformQuery(&status,&selectRel);

    /*prov tbl transition*/
    y_fromTo_tbl(d,selectRel.tbl,tn2);
    y_fromTo_tbl(d,tn2,tn);

    //back
    ListCell* lc;
    int c=0;
    foreach(lc,cns)
    {
        y_cNode cn=lfirst(lc);
        y_cNode ccn=selectRel.col_nodes[c++];
        /*prov col transition*/
        y_fromTo_col(d,ccn,cn);
    }

    y_doSave_tbl(d,tn);

    _y_rtable_fini(&selectRel);

    _y_nodesList_destroy(cns);

    _y_status_fini(&status);
}

void _y_do_prov(y_prov* prov,y_prov_data d,Query* q)
{
    Y_PRINT0("y_do_prov\n");

    if(IsA(q,Query))
    {
        if(q->utilityStmt)
        {
            y_transformUtilityStmt(prov,d,q);
        }
        else
        {
            //FIX ME
            /*
#ifdef Y_PROV_ALLOW_NO_ROOT
            y_status status;
            y_rtable selectRel;
            _y_status_init(&status,prov,d,q,
                           list_length(q->cteList),
                           list_length(q->rtable));
            y_transformQuery(&status,&selectRel);

            _y_rtable_fini(&selectRel);
            _y_status_fini(&status);
#endif
             */
        }
        y_prov_data_saveAll(d);
    }

    Y_PRINT0("y_do_prov done \n");
}
