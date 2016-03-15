//
// Created by bon on 15-6-1.
//
#include "y_io.h"
#include <stdarg.h>

FILE* y_print_f=0;

FILE* y_std_fopen(const char* path, const char* m)
{
	return fopen(path,m);
}

FILE* y_std_refopen(const char* p, const char* m, FILE* f)
{
	return freopen(p,m,f);
}

void y_std_fclose(FILE* f)
{
	fclose(f);
}
int y_std_fprintf(FILE* f, const char* fmt, ...)
{
	int x;
	va_list vas;
	va_start(vas, fmt);
	x=vfprintf(f, fmt, vas);
	va_end(vas);
	return x;
}

int y_std_fflush(FILE* f)
{
	return fflush(f);
}
