//
// Created by bon on 15-6-24.
//

#include "y_str.h"
#include <memory.h>
#include <stdarg.h>

static y_str_alloc_func yalloc=malloc;
static y_str_free_func yfree=free;

//set the new allocator and return the old one
y_str_alloc_func y_hook_str_alloc_func(y_str_alloc_func alloc)
{
    y_str_alloc_func old=yalloc;
    yalloc=alloc;
    return old;
}
//set the new free function and return the old one
y_str_free_func y_hook_str_free_func(y_str_free_func _free)
{
    y_str_free_func old=yfree;
    yfree=_free;
    return old;
}

#define Y_BUF_ALLOC_SIZE(sz) yalloc(sizeof(y_strBuf)+(sz))
#define Y_ALLOC_SIZE(sz) yalloc(sizeof(y_str)+(sz))

y_str* y_str_alloc(int sz)
{
	if (sz < 0)
		return 0;

	y_str* s = Y_ALLOC_SIZE(sz);
	if (!s)
		return s;
	s->strlen = sz;
	s->str.buf[sz] = '\0';

	return s;
}

inline y_str* _y_str_cpy(const char* ptr,int sz)
{

	y_str* s = Y_ALLOC_SIZE(sz);
	if (!s)
		return s;
	s->strlen = sz;
	memcpy(s->str.buf, ptr, sz);
	s->str.buf[sz] = 0;

	return s;
}

y_str* y_str_alloc_keep_cStr(const char* cstr)
{
    y_str* s;
    if(!cstr)
        return 0;
    int clen=strlen(cstr);

	s = Y_ALLOC_SIZE(0);
	if (!s)
		return s;
	s->strlen = -clen;
	s->str.cstr = (char*)cstr;

	return s;
}

y_str* y_str_alloc_cpy(y_str* str)
{
	return _y_str_cpy(y_str_ptr(str), y_str_len(str));
}

y_str* y_str_alloc_cpy_c(const char* cstr)
{
    if(!cstr)
        return 0;

    int clen=strlen(cstr);

	return _y_str_cpy(cstr, clen);
}

y_str* y_str_alloc_cpy_c2(const char* cstr,int clen)
{
	if (!cstr || clen<0)
		return 0;
	return _y_str_cpy(cstr, clen);
}

y_str* y_str_alloc_cpy_args(const char* fmt,...)
{
    int cnt=strlen(fmt);

    if(cnt==0 || cnt>Y_MAX_ARGS_CNT)
        return 0;

    int i;
    int all_len=0;
    int lens[Y_MAX_ARGS_CNT];
    const char* ptrs[Y_MAX_ARGS_CNT];
    va_list vl;
    const char* cs;
    const y_str* ys;
    y_str* s;
    char* p;

    va_start(vl,fmt);
    for(i=0;i<cnt;++i)
    {
        if(fmt[i]=='c')
        {
            cs=va_arg(vl,const char*);
            lens[i]=strlen(cs);
            ptrs[i]=cs;
        }
        else if(fmt[i]=='y')
        {
            ys=va_arg(vl,const y_str*);
            lens[i]=y_str_len(ys);
            ptrs[i]=y_str_ptr(ys);
        }
        else
        {
            lens[i]=0;
            ptrs[i]=0;
        }
        all_len+=lens[i];
    }
    va_end(vl);

	s = Y_ALLOC_SIZE(all_len);
	if (!s)
		return s;
	s->strlen = all_len;
    s->str.buf[all_len]=0;

    p=s->str.buf;
    for(i=0;i<cnt;++i)
    {
        memcpy(p,ptrs[i],lens[i]);
        p+=lens[i];
    }

    return s;
}

void y_str_destroy(y_str* str)
{
    yfree(str);
}

y_strBuf* y_strBuf_alloc(int sz)
{
    if(sz<=0)
        return 0;
    y_strBuf* buf=Y_BUF_ALLOC_SIZE(sz);
    buf->all=sz;
    buf->used=0;
    buf->buf[0]=0;
    return buf;
}

void y_strBuf_reset(y_strBuf* buf)
{
    buf->used=0;
    buf->buf[0]=0;
}

void y_strBuf_destroy(y_strBuf* buf)
{
    yfree(buf);
}

y_strBuf* y_strBuf_append(y_strBuf* buf,const char* fmt,...)//fmt -> [yc]*
{
    int i;
    int all_len=0;
    int lens[Y_MAX_ARGS_CNT];
    const char* ptrs[Y_MAX_ARGS_CNT];
    va_list vl;
    const char* cs;
    const y_str* ys;
    char* p;
    int cnt=strlen(fmt);

    va_start(vl,fmt);
    for(i=0;i<cnt;++i)
    {
        if(fmt[i]=='c')
        {
            cs=va_arg(vl,const char*);
            lens[i]=strlen(cs);
            ptrs[i]=cs;
        }
        else if(fmt[i]=='y')
        {
            ys=va_arg(vl,const y_str*);
            lens[i]=y_str_len(ys);
            ptrs[i]=y_str_ptr(ys);
        }
        else
        {
            lens[i]=0;
            ptrs[i]=0;
        }
        all_len+=lens[i];
    }
    va_end(vl);

    int free=buf->all-buf->used;

    if(free<all_len)
    {
        //FIX ME
        int new_all=(buf->all>all_len)?buf->all*2:buf->all+all_len;
        y_strBuf* new_buf=Y_BUF_ALLOC_SIZE(new_all);
        if(!new_buf)
            return 0;
        memcpy(new_buf->buf,buf->buf,buf->used);
        new_buf->used=buf->used;
        new_buf->all=new_all;
        y_strBuf_destroy(buf);
        buf=new_buf;
    }

    p=buf->buf+buf->used;
    for(i=0;i<cnt;++i)
    {
        memcpy(p,ptrs[i],lens[i]);
        p+=lens[i];
    }
    buf->used+=all_len;
    buf->buf[buf->used]=0;

    return buf;
}
y_str* y_get_str(y_strBuf* buf)
{
	return _y_str_cpy(buf->buf, buf->used);
}