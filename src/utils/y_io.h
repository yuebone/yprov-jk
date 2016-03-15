//
// Created by bon on 15-6-1.
//

#ifndef PG_PROV_Y_IO_H
#define PG_PROV_Y_IO_H

#include <stdio.h>

/*
 * For debugging
 * */

extern FILE* y_print_f;

extern FILE* y_std_fopen(const char*, const char*);
extern FILE* y_std_refopen(const char* p, const char* m, FILE* f);
extern void y_std_fclose(FILE*);
extern int y_std_fprintf(FILE*,const char*,...);
extern int y_std_fflush(FILE* f);

#define	y_fopen		y_std_fopen
#define	y_fclose	y_std_fclose
#define	y_fflush	y_std_fflush
#define	y_fprintf	y_std_fprintf

#ifdef Y_PRINT_ON

#define Y_PRINT_OPEN(y_print_path)            \
    y_print_f=y_std_fopen(y_str_ptr(y_print_path),"w");  \
    if(!y_print_f){elog(ERROR,"cant open %s",y_str_ptr(y_print_path));}


#define Y_PRINT(fmt,args...)    y_std_fprintf(y_print_f,fmt,args);y_std_fflush(y_print_f)
#define Y_PRINT0(str)           y_std_fprintf(y_print_f,str);y_std_fflush(y_print_f)
#define Y_PRINT1(str,V1)        y_std_fprintf(y_print_f,str,V1);y_std_fflush(y_print_f)
#define Y_PRINT2(str,V1,V2)     y_std_fprintf(y_print_f,str,V1,V2);y_std_fflush(y_print_f)
#define Y_PRINT3(str,V1,V2,V3)  y_std_fprintf(y_print_f,str,V1,V2,V3);y_std_fflush(y_print_f)
#define Y_AT                    Y_PRINT3("@%s,%s,%d\n",__FILE__,__FUNCTION__,__LINE__)

#define Y_PRINT_CLOSE           y_std_fclose(y_print_f)

#else

#define Y_PRINT_OPEN(path)
#define Y_PRINT0(str)
#define Y_PRINT1(str,V1)
#define Y_PRINT2(str,V1,V2)
#define Y_PRINT3(str,V1,V2,V3)
#define Y_AT
#define Y_PRINT_CLOSE

#endif

#endif //PG_PROV_Y_IO_H
