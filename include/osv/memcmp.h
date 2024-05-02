#ifndef FAST_STRING_OP_H
#define FAST_STRING_OP_H
#include <string.h>

void* fast_memcpy(void* dest, const void* source, size_t size);
#ifdef __cplusplus
extern "C" {
#endif
int fast_memcmp(const void *a1, const void *a2, size_t const size);
#ifdef __cplusplus
}
#endif


#endif
