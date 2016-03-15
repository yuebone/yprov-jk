//
// Created by bon on 15-6-24.
//

#ifndef PG_PROV_Y_STR_H
#define PG_PROV_Y_STR_H


#include <stdlib.h>

#define Y_MAX_ARGS_CNT 16

typedef struct
{
    int strlen;//if strlen<0 then ptr is a const char* ptr outside

	union 
	{
		char buf[1];
		char* cstr;
	}str;
}y_str;

#define y_str_len(strP) ((strP)->strlen>0?(strP)->strlen:-((strP)->strlen))
#define y_str_ptr(strP) ((char*)( (strP)->strlen>0 ? (strP)->str.buf : (strP)->str.cstr ))

typedef struct
{
    int used;
    int all;

    char buf[1];
}y_strBuf;

#define y_strBuf_buf(p) ((p)->buf)
#define y_strBuf_len(p) ((p)->used)

y_strBuf* y_strBuf_alloc(int sz);
/*reset strBuf.used to 0*/
void y_strBuf_reset(y_strBuf* buf);
void y_strBuf_destroy(y_strBuf* buf);
y_strBuf* y_strBuf_append(y_strBuf* buf,const char* fmt,...);//fmt -> [yc]*
y_str* y_get_str(y_strBuf* buf);

y_str* y_str_alloc(int sz);
y_str* y_str_alloc_keep_cStr(const char* cstr);
y_str* y_str_alloc_cpy_c(const char* cstr);
y_str* y_str_alloc_cpy_c2(const char* cstr,int len);
y_str* y_str_alloc_cpy(y_str* str);
y_str* y_str_alloc_cpy_args(const char* fmt,...);//fmt -> [yc]*

void y_str_destroy(y_str* str);

typedef void* (*y_str_alloc_func)(size_t) ;
typedef void (*y_str_free_func)(void*);

//set the new allocator and return the old one
y_str_alloc_func y_hook_str_alloc_func(y_str_alloc_func);
//set the new free function and return the old one
y_str_free_func y_hook_str_free_func(y_str_free_func);

#endif //PG_PROV_Y_STR_H
