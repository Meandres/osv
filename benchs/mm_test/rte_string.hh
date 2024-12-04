/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_STRING
#define _RTE_STRING

/**
 * @file
 *
 * Functions for SSE/AVX/AVX2/AVX512 implementation of memcpy().
 */

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <x86intrin.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Copy bytes from one location to another. The locations must not overlap.
 *
 * @note This is implemented as a macro, so it's address should not be taken
 * and care is needed as parameter expressions may be evaluated multiple times.
 *
 * @param dst
 *   Pointer to the destination of the data.
 * @param src
 *   Pointer to the source data.
 * @param n
 *   Number of bytes to copy.
 * @return
 *   Pointer to the destination data.
 */
static inline void *
rte_memcpy(void *dst, const void *src, size_t n);

#ifdef __AVX512F__

#define ALIGNMENT_MASK 0x3F

/**
 * AVX512 implementation below
 */

/**
 * Copy 16 bytes from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov16(uint8_t *dst, const uint8_t *src)
{
	__m128i xmm0;

	xmm0 = _mm_loadu_si128((const __m128i *)src);
	_mm_storeu_si128((__m128i *)dst, xmm0);
}

/**
 * Copy 32 bytes from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov32(uint8_t *dst, const uint8_t *src)
{
	__m256i ymm0;

	ymm0 = _mm256_loadu_si256((const __m256i *)src);
	_mm256_storeu_si256((__m256i *)dst, ymm0);
}

/**
 * Copy 64 bytes from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov64(uint8_t *dst, const uint8_t *src)
{
	__m512i zmm0;

	zmm0 = _mm512_loadu_si512((const void *)src);
	_mm512_storeu_si512((void *)dst, zmm0);
}

/**
 * Copy 128 bytes from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov128(uint8_t *dst, const uint8_t *src)
{
	rte_mov64(dst + 0 * 64, src + 0 * 64);
	rte_mov64(dst + 1 * 64, src + 1 * 64);
}

/**
 * Copy 256 bytes from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov256(uint8_t *dst, const uint8_t *src)
{
	rte_mov64(dst + 0 * 64, src + 0 * 64);
	rte_mov64(dst + 1 * 64, src + 1 * 64);
	rte_mov64(dst + 2 * 64, src + 2 * 64);
	rte_mov64(dst + 3 * 64, src + 3 * 64);
}

/**
 * Copy 128-byte blocks from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov128blocks(uint8_t *dst, const uint8_t *src, size_t n)
{
	__m512i zmm0, zmm1;

	while (n >= 128) {
		zmm0 = _mm512_loadu_si512((const void *)(src + 0 * 64));
		n -= 128;
		zmm1 = _mm512_loadu_si512((const void *)(src + 1 * 64));
		src = src + 128;
		_mm512_storeu_si512((void *)(dst + 0 * 64), zmm0);
		_mm512_storeu_si512((void *)(dst + 1 * 64), zmm1);
		dst = dst + 128;
	}
}

/**
 * Copy 512-byte blocks from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov512blocks(uint8_t *dst, const uint8_t *src, size_t n)
{
	__m512i zmm0, zmm1, zmm2, zmm3, zmm4, zmm5, zmm6, zmm7;

	while (n >= 512) {
		zmm0 = _mm512_loadu_si512((const void *)(src + 0 * 64));
		n -= 512;
		zmm1 = _mm512_loadu_si512((const void *)(src + 1 * 64));
		zmm2 = _mm512_loadu_si512((const void *)(src + 2 * 64));
		zmm3 = _mm512_loadu_si512((const void *)(src + 3 * 64));
		zmm4 = _mm512_loadu_si512((const void *)(src + 4 * 64));
		zmm5 = _mm512_loadu_si512((const void *)(src + 5 * 64));
		zmm6 = _mm512_loadu_si512((const void *)(src + 6 * 64));
		zmm7 = _mm512_loadu_si512((const void *)(src + 7 * 64));
		src = src + 512;
		_mm512_storeu_si512((void *)(dst + 0 * 64), zmm0);
		_mm512_storeu_si512((void *)(dst + 1 * 64), zmm1);
		_mm512_storeu_si512((void *)(dst + 2 * 64), zmm2);
		_mm512_storeu_si512((void *)(dst + 3 * 64), zmm3);
		_mm512_storeu_si512((void *)(dst + 4 * 64), zmm4);
		_mm512_storeu_si512((void *)(dst + 5 * 64), zmm5);
		_mm512_storeu_si512((void *)(dst + 6 * 64), zmm6);
		_mm512_storeu_si512((void *)(dst + 7 * 64), zmm7);
		dst = dst + 512;
	}
}

static inline void *
rte_memcpy_generic(void *dst, const void *src, size_t n)
{
	uintptr_t dstu = (uintptr_t)dst;
	uintptr_t srcu = (uintptr_t)src;
	void *ret = dst;
	size_t dstofss;
	size_t bits;

	/**
	 * Copy less than 16 bytes
	 */
	if (n < 16) {
		if (n & 0x01) {
			*(uint8_t *)dstu = *(const uint8_t *)srcu;
			srcu = (uintptr_t)((const uint8_t *)srcu + 1);
			dstu = (uintptr_t)((uint8_t *)dstu + 1);
		}
		if (n & 0x02) {
			*(uint16_t *)dstu = *(const uint16_t *)srcu;
			srcu = (uintptr_t)((const uint16_t *)srcu + 1);
			dstu = (uintptr_t)((uint16_t *)dstu + 1);
		}
		if (n & 0x04) {
			*(uint32_t *)dstu = *(const uint32_t *)srcu;
			srcu = (uintptr_t)((const uint32_t *)srcu + 1);
			dstu = (uintptr_t)((uint32_t *)dstu + 1);
		}
		if (n & 0x08)
			*(uint64_t *)dstu = *(const uint64_t *)srcu;
		return ret;
	}

	/**
	 * Fast way when copy size doesn't exceed 512 bytes
	 */
	if (n <= 32) {
		rte_mov16((uint8_t *)dst, (const uint8_t *)src);
		rte_mov16((uint8_t *)dst - 16 + n,
				  (const uint8_t *)src - 16 + n);
		return ret;
	}
	if (n <= 64) {
		rte_mov32((uint8_t *)dst, (const uint8_t *)src);
		rte_mov32((uint8_t *)dst - 32 + n,
				  (const uint8_t *)src - 32 + n);
		return ret;
	}
	if (n <= 512) {
		if (n >= 256) {
			n -= 256;
			rte_mov256((uint8_t *)dst, (const uint8_t *)src);
			src = (const uint8_t *)src + 256;
			dst = (uint8_t *)dst + 256;
		}
		if (n >= 128) {
			n -= 128;
			rte_mov128((uint8_t *)dst, (const uint8_t *)src);
			src = (const uint8_t *)src + 128;
			dst = (uint8_t *)dst + 128;
		}
COPY_BLOCK_128_BACK63:
		if (n > 64) {
			rte_mov64((uint8_t *)dst, (const uint8_t *)src);
			rte_mov64((uint8_t *)dst - 64 + n,
					  (const uint8_t *)src - 64 + n);
			return ret;
		}
		if (n > 0)
			rte_mov64((uint8_t *)dst - 64 + n,
					  (const uint8_t *)src - 64 + n);
		return ret;
	}

	/**
	 * Make store aligned when copy size exceeds 512 bytes
	 */
	dstofss = ((uintptr_t)dst & 0x3F);
	if (dstofss > 0) {
		dstofss = 64 - dstofss;
		n -= dstofss;
		rte_mov64((uint8_t *)dst, (const uint8_t *)src);
		src = (const uint8_t *)src + dstofss;
		dst = (uint8_t *)dst + dstofss;
	}

	/**
	 * Copy 512-byte blocks.
	 * Use copy block function for better instruction order control,
	 * which is important when load is unaligned.
	 */
	rte_mov512blocks((uint8_t *)dst, (const uint8_t *)src, n);
	bits = n;
	n = n & 511;
	bits -= n;
	src = (const uint8_t *)src + bits;
	dst = (uint8_t *)dst + bits;

	/**
	 * Copy 128-byte blocks.
	 * Use copy block function for better instruction order control,
	 * which is important when load is unaligned.
	 */
	if (n >= 128) {
		rte_mov128blocks((uint8_t *)dst, (const uint8_t *)src, n);
		bits = n;
		n = n & 127;
		bits -= n;
		src = (const uint8_t *)src + bits;
		dst = (uint8_t *)dst + bits;
	}

	/**
	 * Copy whatever left
	 */
	goto COPY_BLOCK_128_BACK63;
}

#elif defined __AVX2__

#define ALIGNMENT_MASK 0x1F

/**
 * AVX2 implementation below
 */

/**
 * Copy 16 bytes from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov16(uint8_t *dst, const uint8_t *src)
{
	__m128i xmm0;

	xmm0 = _mm_loadu_si128((const __m128i *)src);
	_mm_storeu_si128((__m128i *)dst, xmm0);
}

/**
 * Copy 32 bytes from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov32(uint8_t *dst, const uint8_t *src)
{
	__m256i ymm0;

	ymm0 = _mm256_loadu_si256((const __m256i *)src);
	_mm256_storeu_si256((__m256i *)dst, ymm0);
}

/**
 * Copy 64 bytes from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov64(uint8_t *dst, const uint8_t *src)
{
	rte_mov32((uint8_t *)dst + 0 * 32, (const uint8_t *)src + 0 * 32);
	rte_mov32((uint8_t *)dst + 1 * 32, (const uint8_t *)src + 1 * 32);
}

/**
 * Copy 128 bytes from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov128(uint8_t *dst, const uint8_t *src)
{
	rte_mov32((uint8_t *)dst + 0 * 32, (const uint8_t *)src + 0 * 32);
	rte_mov32((uint8_t *)dst + 1 * 32, (const uint8_t *)src + 1 * 32);
	rte_mov32((uint8_t *)dst + 2 * 32, (const uint8_t *)src + 2 * 32);
	rte_mov32((uint8_t *)dst + 3 * 32, (const uint8_t *)src + 3 * 32);
}

/**
 * Copy 128-byte blocks from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov128blocks(uint8_t *dst, const uint8_t *src, size_t n)
{
	__m256i ymm0, ymm1, ymm2, ymm3;

	while (n >= 128) {
		ymm0 = _mm256_loadu_si256((const __m256i *)((const uint8_t *)src + 0 * 32));
		n -= 128;
		ymm1 = _mm256_loadu_si256((const __m256i *)((const uint8_t *)src + 1 * 32));
		ymm2 = _mm256_loadu_si256((const __m256i *)((const uint8_t *)src + 2 * 32));
		ymm3 = _mm256_loadu_si256((const __m256i *)((const uint8_t *)src + 3 * 32));
		src = (const uint8_t *)src + 128;
		_mm256_storeu_si256((__m256i *)((uint8_t *)dst + 0 * 32), ymm0);
		_mm256_storeu_si256((__m256i *)((uint8_t *)dst + 1 * 32), ymm1);
		_mm256_storeu_si256((__m256i *)((uint8_t *)dst + 2 * 32), ymm2);
		_mm256_storeu_si256((__m256i *)((uint8_t *)dst + 3 * 32), ymm3);
		dst = (uint8_t *)dst + 128;
	}
}

static inline void *
rte_memcpy_generic(void *dst, const void *src, size_t n)
{
	uintptr_t dstu = (uintptr_t)dst;
	uintptr_t srcu = (uintptr_t)src;
	void *ret = dst;
	size_t dstofss;
	size_t bits;

	/**
	 * Copy less than 16 bytes
	 */
	if (n < 16) {
		if (n & 0x01) {
			*(uint8_t *)dstu = *(const uint8_t *)srcu;
			srcu = (uintptr_t)((const uint8_t *)srcu + 1);
			dstu = (uintptr_t)((uint8_t *)dstu + 1);
		}
		if (n & 0x02) {
			*(uint16_t *)dstu = *(const uint16_t *)srcu;
			srcu = (uintptr_t)((const uint16_t *)srcu + 1);
			dstu = (uintptr_t)((uint16_t *)dstu + 1);
		}
		if (n & 0x04) {
			*(uint32_t *)dstu = *(const uint32_t *)srcu;
			srcu = (uintptr_t)((const uint32_t *)srcu + 1);
			dstu = (uintptr_t)((uint32_t *)dstu + 1);
		}
		if (n & 0x08) {
			*(uint64_t *)dstu = *(const uint64_t *)srcu;
		}
		return ret;
	}

	/**
	 * Fast way when copy size doesn't exceed 256 bytes
	 */
	if (n <= 32) {
		rte_mov16((uint8_t *)dst, (const uint8_t *)src);
		rte_mov16((uint8_t *)dst - 16 + n,
				(const uint8_t *)src - 16 + n);
		return ret;
	}
	if (n <= 48) {
		rte_mov16((uint8_t *)dst, (const uint8_t *)src);
		rte_mov16((uint8_t *)dst + 16, (const uint8_t *)src + 16);
		rte_mov16((uint8_t *)dst - 16 + n,
				(const uint8_t *)src - 16 + n);
		return ret;
	}
	if (n <= 64) {
		rte_mov32((uint8_t *)dst, (const uint8_t *)src);
		rte_mov32((uint8_t *)dst - 32 + n,
				(const uint8_t *)src - 32 + n);
		return ret;
	}
	if (n <= 256) {
		if (n >= 128) {
			n -= 128;
			rte_mov128((uint8_t *)dst, (const uint8_t *)src);
			src = (const uint8_t *)src + 128;
			dst = (uint8_t *)dst + 128;
		}
COPY_BLOCK_128_BACK31:
		if (n >= 64) {
			n -= 64;
			rte_mov64((uint8_t *)dst, (const uint8_t *)src);
			src = (const uint8_t *)src + 64;
			dst = (uint8_t *)dst + 64;
		}
		if (n > 32) {
			rte_mov32((uint8_t *)dst, (const uint8_t *)src);
			rte_mov32((uint8_t *)dst - 32 + n,
					(const uint8_t *)src - 32 + n);
			return ret;
		}
		if (n > 0) {
			rte_mov32((uint8_t *)dst - 32 + n,
					(const uint8_t *)src - 32 + n);
		}
		return ret;
	}

	/**
	 * Make store aligned when copy size exceeds 256 bytes
	 */
	dstofss = (uintptr_t)dst & 0x1F;
	if (dstofss > 0) {
		dstofss = 32 - dstofss;
		n -= dstofss;
		rte_mov32((uint8_t *)dst, (const uint8_t *)src);
		src = (const uint8_t *)src + dstofss;
		dst = (uint8_t *)dst + dstofss;
	}

	/**
	 * Copy 128-byte blocks
	 */
	rte_mov128blocks((uint8_t *)dst, (const uint8_t *)src, n);
	bits = n;
	n = n & 127;
	bits -= n;
	src = (const uint8_t *)src + bits;
	dst = (uint8_t *)dst + bits;

	/**
	 * Copy whatever left
	 */
	goto COPY_BLOCK_128_BACK31;
}

#else /* RTE_MACHINE_CPUFLAG */

#define ALIGNMENT_MASK 0x0F

/**
 * SSE & AVX implementation below
 */

/**
 * Copy 16 bytes from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov16(uint8_t *dst, const uint8_t *src)
{
	__m128i xmm0;

	xmm0 = _mm_loadu_si128((const __m128i *)(const __m128i *)src);
	_mm_storeu_si128((__m128i *)dst, xmm0);
}

/**
 * Copy 32 bytes from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov32(uint8_t *dst, const uint8_t *src)
{
	rte_mov16((uint8_t *)dst + 0 * 16, (const uint8_t *)src + 0 * 16);
	rte_mov16((uint8_t *)dst + 1 * 16, (const uint8_t *)src + 1 * 16);
}

/**
 * Copy 64 bytes from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov64(uint8_t *dst, const uint8_t *src)
{
	rte_mov16((uint8_t *)dst + 0 * 16, (const uint8_t *)src + 0 * 16);
	rte_mov16((uint8_t *)dst + 1 * 16, (const uint8_t *)src + 1 * 16);
	rte_mov16((uint8_t *)dst + 2 * 16, (const uint8_t *)src + 2 * 16);
	rte_mov16((uint8_t *)dst + 3 * 16, (const uint8_t *)src + 3 * 16);
}

/**
 * Copy 128 bytes from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov128(uint8_t *dst, const uint8_t *src)
{
	rte_mov16((uint8_t *)dst + 0 * 16, (const uint8_t *)src + 0 * 16);
	rte_mov16((uint8_t *)dst + 1 * 16, (const uint8_t *)src + 1 * 16);
	rte_mov16((uint8_t *)dst + 2 * 16, (const uint8_t *)src + 2 * 16);
	rte_mov16((uint8_t *)dst + 3 * 16, (const uint8_t *)src + 3 * 16);
	rte_mov16((uint8_t *)dst + 4 * 16, (const uint8_t *)src + 4 * 16);
	rte_mov16((uint8_t *)dst + 5 * 16, (const uint8_t *)src + 5 * 16);
	rte_mov16((uint8_t *)dst + 6 * 16, (const uint8_t *)src + 6 * 16);
	rte_mov16((uint8_t *)dst + 7 * 16, (const uint8_t *)src + 7 * 16);
}

/**
 * Copy 256 bytes from one location to another,
 * locations should not overlap.
 */
static inline void
rte_mov256(uint8_t *dst, const uint8_t *src)
{
	rte_mov16((uint8_t *)dst + 0 * 16, (const uint8_t *)src + 0 * 16);
	rte_mov16((uint8_t *)dst + 1 * 16, (const uint8_t *)src + 1 * 16);
	rte_mov16((uint8_t *)dst + 2 * 16, (const uint8_t *)src + 2 * 16);
	rte_mov16((uint8_t *)dst + 3 * 16, (const uint8_t *)src + 3 * 16);
	rte_mov16((uint8_t *)dst + 4 * 16, (const uint8_t *)src + 4 * 16);
	rte_mov16((uint8_t *)dst + 5 * 16, (const uint8_t *)src + 5 * 16);
	rte_mov16((uint8_t *)dst + 6 * 16, (const uint8_t *)src + 6 * 16);
	rte_mov16((uint8_t *)dst + 7 * 16, (const uint8_t *)src + 7 * 16);
	rte_mov16((uint8_t *)dst + 8 * 16, (const uint8_t *)src + 8 * 16);
	rte_mov16((uint8_t *)dst + 9 * 16, (const uint8_t *)src + 9 * 16);
	rte_mov16((uint8_t *)dst + 10 * 16, (const uint8_t *)src + 10 * 16);
	rte_mov16((uint8_t *)dst + 11 * 16, (const uint8_t *)src + 11 * 16);
	rte_mov16((uint8_t *)dst + 12 * 16, (const uint8_t *)src + 12 * 16);
	rte_mov16((uint8_t *)dst + 13 * 16, (const uint8_t *)src + 13 * 16);
	rte_mov16((uint8_t *)dst + 14 * 16, (const uint8_t *)src + 14 * 16);
	rte_mov16((uint8_t *)dst + 15 * 16, (const uint8_t *)src + 15 * 16);
}

/**
 * Macro for copying unaligned block from one location to another with constant load offset,
 * 47 bytes leftover maximum,
 * locations should not overlap.
 * Requirements:
 * - Store is aligned
 * - Load offset is <offset>, which must be immediate value within [1, 15]
 * - For <src>, make sure <offset> bit backwards & <16 - offset> bit forwards are available for loading
 * - <dst>, <src>, <len> must be variables
 * - __m128i <xmm0> ~ <xmm8> must be pre-defined
 */
#define MOVEUNALIGNED_LEFT47_IMM(dst, src, len, offset)                                                     \
__extension__ ({                                                                                            \
    size_t tmp;                                                                                                \
    while (len >= 128 + 16 - offset) {                                                                      \
        xmm0 = _mm_loadu_si128((const __m128i *)((const uint8_t *)src - offset + 0 * 16));                  \
        len -= 128;                                                                                         \
        xmm1 = _mm_loadu_si128((const __m128i *)((const uint8_t *)src - offset + 1 * 16));                  \
        xmm2 = _mm_loadu_si128((const __m128i *)((const uint8_t *)src - offset + 2 * 16));                  \
        xmm3 = _mm_loadu_si128((const __m128i *)((const uint8_t *)src - offset + 3 * 16));                  \
        xmm4 = _mm_loadu_si128((const __m128i *)((const uint8_t *)src - offset + 4 * 16));                  \
        xmm5 = _mm_loadu_si128((const __m128i *)((const uint8_t *)src - offset + 5 * 16));                  \
        xmm6 = _mm_loadu_si128((const __m128i *)((const uint8_t *)src - offset + 6 * 16));                  \
        xmm7 = _mm_loadu_si128((const __m128i *)((const uint8_t *)src - offset + 7 * 16));                  \
        xmm8 = _mm_loadu_si128((const __m128i *)((const uint8_t *)src - offset + 8 * 16));                  \
        src = (const uint8_t *)src + 128;                                                                   \
        _mm_storeu_si128((__m128i *)((uint8_t *)dst + 0 * 16), _mm_alignr_epi8(xmm1, xmm0, offset));        \
        _mm_storeu_si128((__m128i *)((uint8_t *)dst + 1 * 16), _mm_alignr_epi8(xmm2, xmm1, offset));        \
        _mm_storeu_si128((__m128i *)((uint8_t *)dst + 2 * 16), _mm_alignr_epi8(xmm3, xmm2, offset));        \
        _mm_storeu_si128((__m128i *)((uint8_t *)dst + 3 * 16), _mm_alignr_epi8(xmm4, xmm3, offset));        \
        _mm_storeu_si128((__m128i *)((uint8_t *)dst + 4 * 16), _mm_alignr_epi8(xmm5, xmm4, offset));        \
        _mm_storeu_si128((__m128i *)((uint8_t *)dst + 5 * 16), _mm_alignr_epi8(xmm6, xmm5, offset));        \
        _mm_storeu_si128((__m128i *)((uint8_t *)dst + 6 * 16), _mm_alignr_epi8(xmm7, xmm6, offset));        \
        _mm_storeu_si128((__m128i *)((uint8_t *)dst + 7 * 16), _mm_alignr_epi8(xmm8, xmm7, offset));        \
        dst = (uint8_t *)dst + 128;                                                                         \
    }                                                                                                       \
    tmp = len;                                                                                              \
    len = ((len - 16 + offset) & 127) + 16 - offset;                                                        \
    tmp -= len;                                                                                             \
    src = (const uint8_t *)src + tmp;                                                                       \
    dst = (uint8_t *)dst + tmp;                                                                             \
    if (len >= 32 + 16 - offset) {                                                                          \
        while (len >= 32 + 16 - offset) {                                                                   \
            xmm0 = _mm_loadu_si128((const __m128i *)((const uint8_t *)src - offset + 0 * 16));              \
            len -= 32;                                                                                      \
            xmm1 = _mm_loadu_si128((const __m128i *)((const uint8_t *)src - offset + 1 * 16));              \
            xmm2 = _mm_loadu_si128((const __m128i *)((const uint8_t *)src - offset + 2 * 16));              \
            src = (const uint8_t *)src + 32;                                                                \
            _mm_storeu_si128((__m128i *)((uint8_t *)dst + 0 * 16), _mm_alignr_epi8(xmm1, xmm0, offset));    \
            _mm_storeu_si128((__m128i *)((uint8_t *)dst + 1 * 16), _mm_alignr_epi8(xmm2, xmm1, offset));    \
            dst = (uint8_t *)dst + 32;                                                                      \
        }                                                                                                   \
        tmp = len;                                                                                          \
        len = ((len - 16 + offset) & 31) + 16 - offset;                                                     \
        tmp -= len;                                                                                         \
        src = (const uint8_t *)src + tmp;                                                                   \
        dst = (uint8_t *)dst + tmp;                                                                         \
    }                                                                                                       \
})

/**
 * Macro for copying unaligned block from one location to another,
 * 47 bytes leftover maximum,
 * locations should not overlap.
 * Use switch here because the aligning instruction requires immediate value for shift count.
 * Requirements:
 * - Store is aligned
 * - Load offset is <offset>, which must be within [1, 15]
 * - For <src>, make sure <offset> bit backwards & <16 - offset> bit forwards are available for loading
 * - <dst>, <src>, <len> must be variables
 * - __m128i <xmm0> ~ <xmm8> used in MOVEUNALIGNED_LEFT47_IMM must be pre-defined
 */
#define MOVEUNALIGNED_LEFT47(dst, src, len, offset)                   \
__extension__ ({                                                      \
    switch (offset) {                                                 \
    case 0x01: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x01); break;    \
    case 0x02: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x02); break;    \
    case 0x03: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x03); break;    \
    case 0x04: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x04); break;    \
    case 0x05: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x05); break;    \
    case 0x06: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x06); break;    \
    case 0x07: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x07); break;    \
    case 0x08: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x08); break;    \
    case 0x09: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x09); break;    \
    case 0x0A: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x0A); break;    \
    case 0x0B: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x0B); break;    \
    case 0x0C: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x0C); break;    \
    case 0x0D: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x0D); break;    \
    case 0x0E: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x0E); break;    \
    case 0x0F: MOVEUNALIGNED_LEFT47_IMM(dst, src, n, 0x0F); break;    \
    default:;                                                         \
    }                                                                 \
})

static inline void *
rte_memcpy_generic(void *dst, const void *src, size_t n)
{
	__m128i xmm0, xmm1, xmm2, xmm3, xmm4, xmm5, xmm6, xmm7, xmm8;
	uintptr_t dstu = (uintptr_t)dst;
	uintptr_t srcu = (uintptr_t)src;
	void *ret = dst;
	size_t dstofss;
	size_t srcofs;

	/**
	 * Copy less than 16 bytes
	 */
	if (n < 16) {
		if (n & 0x01) {
			*(uint8_t *)dstu = *(const uint8_t *)srcu;
			srcu = (uintptr_t)((const uint8_t *)srcu + 1);
			dstu = (uintptr_t)((uint8_t *)dstu + 1);
		}
		if (n & 0x02) {
			*(uint16_t *)dstu = *(const uint16_t *)srcu;
			srcu = (uintptr_t)((const uint16_t *)srcu + 1);
			dstu = (uintptr_t)((uint16_t *)dstu + 1);
		}
		if (n & 0x04) {
			*(uint32_t *)dstu = *(const uint32_t *)srcu;
			srcu = (uintptr_t)((const uint32_t *)srcu + 1);
			dstu = (uintptr_t)((uint32_t *)dstu + 1);
		}
		if (n & 0x08) {
			*(uint64_t *)dstu = *(const uint64_t *)srcu;
		}
		return ret;
	}

	/**
	 * Fast way when copy size doesn't exceed 512 bytes
	 */
	if (n <= 32) {
		rte_mov16((uint8_t *)dst, (const uint8_t *)src);
		rte_mov16((uint8_t *)dst - 16 + n, (const uint8_t *)src - 16 + n);
		return ret;
	}
	if (n <= 48) {
		rte_mov32((uint8_t *)dst, (const uint8_t *)src);
		rte_mov16((uint8_t *)dst - 16 + n, (const uint8_t *)src - 16 + n);
		return ret;
	}
	if (n <= 64) {
		rte_mov32((uint8_t *)dst, (const uint8_t *)src);
		rte_mov16((uint8_t *)dst + 32, (const uint8_t *)src + 32);
		rte_mov16((uint8_t *)dst - 16 + n, (const uint8_t *)src - 16 + n);
		return ret;
	}
	if (n <= 128) {
		goto COPY_BLOCK_128_BACK15;
	}
	if (n <= 512) {
		if (n >= 256) {
			n -= 256;
			rte_mov128((uint8_t *)dst, (const uint8_t *)src);
			rte_mov128((uint8_t *)dst + 128, (const uint8_t *)src + 128);
			src = (const uint8_t *)src + 256;
			dst = (uint8_t *)dst + 256;
		}
COPY_BLOCK_255_BACK15:
		if (n >= 128) {
			n -= 128;
			rte_mov128((uint8_t *)dst, (const uint8_t *)src);
			src = (const uint8_t *)src + 128;
			dst = (uint8_t *)dst + 128;
		}
COPY_BLOCK_128_BACK15:
		if (n >= 64) {
			n -= 64;
			rte_mov64((uint8_t *)dst, (const uint8_t *)src);
			src = (const uint8_t *)src + 64;
			dst = (uint8_t *)dst + 64;
		}
COPY_BLOCK_64_BACK15:
		if (n >= 32) {
			n -= 32;
			rte_mov32((uint8_t *)dst, (const uint8_t *)src);
			src = (const uint8_t *)src + 32;
			dst = (uint8_t *)dst + 32;
		}
		if (n > 16) {
			rte_mov16((uint8_t *)dst, (const uint8_t *)src);
			rte_mov16((uint8_t *)dst - 16 + n, (const uint8_t *)src - 16 + n);
			return ret;
		}
		if (n > 0) {
			rte_mov16((uint8_t *)dst - 16 + n, (const uint8_t *)src - 16 + n);
		}
		return ret;
	}

	/**
	 * Make store aligned when copy size exceeds 512 bytes,
	 * and make sure the first 15 bytes are copied, because
	 * unaligned copy functions require up to 15 bytes
	 * backwards access.
	 */
	dstofss = (uintptr_t)dst & 0x0F;
	if (dstofss > 0) {
		dstofss = 16 - dstofss + 16;
		n -= dstofss;
		rte_mov32((uint8_t *)dst, (const uint8_t *)src);
		src = (const uint8_t *)src + dstofss;
		dst = (uint8_t *)dst + dstofss;
	}
	srcofs = ((uintptr_t)src & 0x0F);

	/**
	 * For aligned copy
	 */
	if (srcofs == 0) {
		/**
		 * Copy 256-byte blocks
		 */
		for (; n >= 256; n -= 256) {
			rte_mov256((uint8_t *)dst, (const uint8_t *)src);
			dst = (uint8_t *)dst + 256;
			src = (const uint8_t *)src + 256;
		}

		/**
		 * Copy whatever left
		 */
		goto COPY_BLOCK_255_BACK15;
	}

	/**
	 * For copy with unaligned load
	 */
	MOVEUNALIGNED_LEFT47(dst, src, n, srcofs);

	/**
	 * Copy whatever left
	 */
	goto COPY_BLOCK_64_BACK15;
}

#endif /* RTE_MACHINE_CPUFLAG */

static inline void *
rte_memcpy_aligned(void *dst, const void *src, size_t n)
{
	void *ret = dst;

	/* Copy size <= 16 bytes */
	if (n < 16) {
		if (n & 0x01) {
			*(uint8_t *)dst = *(const uint8_t *)src;
			src = (const uint8_t *)src + 1;
			dst = (uint8_t *)dst + 1;
		}
		if (n & 0x02) {
			*(uint16_t *)dst = *(const uint16_t *)src;
			src = (const uint16_t *)src + 1;
			dst = (uint16_t *)dst + 1;
		}
		if (n & 0x04) {
			*(uint32_t *)dst = *(const uint32_t *)src;
			src = (const uint32_t *)src + 1;
			dst = (uint32_t *)dst + 1;
		}
		if (n & 0x08)
			*(uint64_t *)dst = *(const uint64_t *)src;

		return ret;
	}

	/* Copy 16 <= size <= 32 bytes */
	if (n <= 32) {
		rte_mov16((uint8_t *)dst, (const uint8_t *)src);
		rte_mov16((uint8_t *)dst - 16 + n,
				(const uint8_t *)src - 16 + n);

		return ret;
	}

	/* Copy 32 < size <= 64 bytes */
	if (n <= 64) {
		rte_mov32((uint8_t *)dst, (const uint8_t *)src);
		rte_mov32((uint8_t *)dst - 32 + n,
				(const uint8_t *)src - 32 + n);

		return ret;
	}

	/* Copy 64 bytes blocks */
	for (; n >= 64; n -= 64) {
		rte_mov64((uint8_t *)dst, (const uint8_t *)src);
		dst = (uint8_t *)dst + 64;
		src = (const uint8_t *)src + 64;
	}

	/* Copy whatever left */
	rte_mov64((uint8_t *)dst - 64 + n,
			(const uint8_t *)src - 64 + n);

	return ret;
}

static inline void *
rte_memcpy(void *dst, const void *src, size_t n)
{
	if (!(((uintptr_t)dst | (uintptr_t)src) & ALIGNMENT_MASK))
		return rte_memcpy_aligned(dst, src, n);
	else
		return rte_memcpy_generic(dst, src, n);
}

#define likely(condition) __builtin_expect(condition, 1)
#define unlikely(condition) __builtin_expect(condition, 0)

/**
 * Compare bytes between two locations. The locations must not overlap.
 *
 * @param src_1
 *   Pointer to the first source of the data.
 * @param src_2
 *   Pointer to the second source of the data.
 * @param n
 *   Number of bytes to compare.
 * @return
 *   zero if src_1 equal src_2
 *   -ve if src_1 less than src_2
 *   +ve if src_1 greater than src_2
 */
static inline int
rte_memcmp(const void *src_1, const void *src,
        size_t n) __attribute__((always_inline));

/**
 * Find the first different bit for comparison.
 */
static inline int
rte_cmpffd (uint32_t x, uint32_t y)
{
    int i;
    int pos = x ^ y;
    for (i = 0; i < 32; i++)
        if (pos & (1<<i))
            return i;
    return -1;
}

/**
 * Find the first different byte for comparison.
 */
static inline int
rte_cmpffdb (const uint8_t *x, const uint8_t *y, size_t n)
{
    size_t i;
    for (i = 0; i < n; i++)
        if (x[i] != y[i])
            return x[i] - y[i];
    return 0;
}

/**
 * Compare 16 bytes between two locations.
 * locations should not overlap.
 */
static inline int
rte_cmp16(const void *src_1, const void *src_2)
{
    __m128i xmm0, xmm1, xmm2;

    xmm0 = _mm_lddqu_si128((const __m128i *)src_1);
    xmm1 = _mm_lddqu_si128((const __m128i *)src_2);
    xmm2 = _mm_xor_si128(xmm0, xmm1);

    if (unlikely(!_mm_testz_si128(xmm2, xmm2))) {

        uint64_t mm11 = _mm_extract_epi64(xmm0, 0);
        uint64_t mm12 = _mm_extract_epi64(xmm0, 1);

        uint64_t mm21 = _mm_extract_epi64(xmm1, 0);
        uint64_t mm22 = _mm_extract_epi64(xmm1, 1);

        if (mm11 == mm21)
            return rte_cmpffdb((const uint8_t *)&mm12,
                    (const uint8_t *)&mm22, 8);
        else
            return rte_cmpffdb((const uint8_t *)&mm11,
                    (const uint8_t *)&mm21, 8);
    }

    return 0;
}

/**
 * Compare 0 to 15 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_memcmp_regular(const uint8_t *src_1u, const uint8_t *src_2u, size_t n)
{
    int ret = 1;

    /**
     * Compare less than 16 bytes
     */
    if (n & 0x08) {
        ret = (*(const uint64_t *)src_1u ==
                *(const uint64_t *)src_2u);

        if ((ret != 1))
            goto exit_8;

        n -= 0x8;
        src_1u += 0x8;
        src_2u += 0x8;
    }

    if (n & 0x04) {
        ret = (*(const uint32_t *)src_1u ==
                *(const uint32_t *)src_2u);

        if ((ret != 1))
            goto exit_4;

        n -= 0x4;
        src_1u += 0x4;
        src_2u += 0x4;
    }

    if (n & 0x02) {
        ret = (*(const uint16_t *)src_1u ==
                *(const uint16_t *)src_2u);

        if ((ret != 1))
            goto exit_2;

        n -= 0x2;
        src_1u += 0x2;
        src_2u += 0x2;
    }

    if (n & 0x01) {
        ret = (*(const uint8_t *)src_1u ==
                *(const uint8_t *)src_2u);

        if ((ret != 1))
            goto exit_1;

        n -= 0x1;
        src_1u += 0x1;
        src_2u += 0x1;
    }

    return !ret;

exit_8:
    return rte_cmpffdb(src_1u, src_2u, 8);
exit_4:
    return rte_cmpffdb(src_1u, src_2u, 4);
exit_2:
    return rte_cmpffdb(src_1u, src_2u, 2);
exit_1:
    return rte_cmpffdb(src_1u, src_2u, 1);
}

/**
 * AVX2 implementation below
 */

/**
 * Compare 32 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp32(const void *src_1, const void *src_2)
{
    const __m128i* src1 = (const __m128i*)src_1;
    const __m128i* src2 = (const __m128i*)src_2;
    const uint8_t *s1, *s2;

    __m128i mm11 = _mm_lddqu_si128(src1);
    __m128i mm12 = _mm_lddqu_si128(src1 + 1);
    __m128i mm21 = _mm_lddqu_si128(src2);
    __m128i mm22 = _mm_lddqu_si128(src2 + 1);

    __m128i mm1 = _mm_xor_si128(mm11, mm21);
    __m128i mm2 = _mm_xor_si128(mm12, mm22);
    __m128i mm = _mm_or_si128(mm1, mm2);

    if (unlikely(!_mm_testz_si128(mm, mm))) {

        /*
         * Find out which of the two 16-byte blocks
         * are different.
         */
        if (_mm_testz_si128(mm1, mm1)) {
            mm11 = mm12;
            mm21 = mm22;
            mm1 = mm2;
            s1 = (const uint8_t *)(src1 + 1);
            s2 = (const uint8_t *)(src2 + 1);
        } else {
            s1 = (const uint8_t *)src1;
            s2 = (const uint8_t *)src2;
        }

        // Produce the comparison result
        __m128i mm_cmp = _mm_cmpgt_epi8(mm11, mm21);
        __m128i mm_rcmp = _mm_cmpgt_epi8(mm21, mm11);
        mm_cmp = _mm_xor_si128(mm1, mm_cmp);
        mm_rcmp = _mm_xor_si128(mm1, mm_rcmp);

        uint32_t cmp = _mm_movemask_epi8(mm_cmp);
        uint32_t rcmp = _mm_movemask_epi8(mm_rcmp);

        int cmp_b = rte_cmpffd(cmp, rcmp);

        int ret = (cmp_b == -1) ? 0 : (s1[cmp_b] - s2[cmp_b]);
        return ret;
    }

    return 0;
}

/**
 * Compare 48 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp48(const void *src_1, const void *src_2)
{
    int ret;

    ret = rte_cmp32((const uint8_t *)src_1 + 0 * 32,
            (const uint8_t *)src_2 + 0 * 32);

    if (unlikely(ret != 0))
        return ret;

    ret = rte_cmp16((const uint8_t *)src_1 + 1 * 32,
            (const uint8_t *)src_2 + 1 * 32);
    return ret;
}

/**
 * Compare 64 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp64 (const void* src_1, const void* src_2)
{
    const __m256i* src1 = (const __m256i*)src_1;
    const __m256i* src2 = (const __m256i*)src_2;
    const uint8_t *s1, *s2;

    __m256i mm11 = _mm256_lddqu_si256(src1);
    __m256i mm12 = _mm256_lddqu_si256(src1 + 1);
    __m256i mm21 = _mm256_lddqu_si256(src2);
    __m256i mm22 = _mm256_lddqu_si256(src2 + 1);

    __m256i mm1 = _mm256_xor_si256(mm11, mm21);
    __m256i mm2 = _mm256_xor_si256(mm12, mm22);
    __m256i mm = _mm256_or_si256(mm1, mm2);

    if (unlikely(!_mm256_testz_si256(mm, mm))) {
        /*
         * Find out which of the two 32-byte blocks
         * are different.
         */
        if (_mm256_testz_si256(mm1, mm1)) {
            mm11 = mm12;
            mm21 = mm22;
            mm1 = mm2;
            s1 = (const uint8_t *)(src1 + 1);
            s2 = (const uint8_t *)(src2 + 1);
        } else {
            s1 = (const uint8_t *)src1;
            s2 = (const uint8_t *)src2;
        }

        // Produce the comparison result
        __m256i mm_cmp = _mm256_cmpgt_epi8(mm11, mm21);
        __m256i mm_rcmp = _mm256_cmpgt_epi8(mm21, mm11);
        mm_cmp = _mm256_xor_si256(mm1, mm_cmp);
        mm_rcmp = _mm256_xor_si256(mm1, mm_rcmp);

        uint32_t cmp = _mm256_movemask_epi8(mm_cmp);
        uint32_t rcmp = _mm256_movemask_epi8(mm_rcmp);

        int cmp_b = rte_cmpffd(cmp, rcmp);

        int ret = (cmp_b == -1) ? 0 : (s1[cmp_b] - s2[cmp_b]);
        return ret;
    }

    return 0;
}

/**
 * Compare 128 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp128(const void *src_1, const void *src_2)
{
    int ret;

    ret = rte_cmp64((const uint8_t *)src_1 + 0 * 64,
            (const uint8_t *)src_2 + 0 * 64);

    if (unlikely(ret != 0))
        return ret;

    return rte_cmp64((const uint8_t *)src_1 + 1 * 64,
            (const uint8_t *)src_2 + 1 * 64);
}

/**
 * Compare 256 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp256(const void *src_1, const void *src_2)
{
    int ret;

    ret = rte_cmp64((const uint8_t *)src_1 + 0 * 64,
            (const uint8_t *)src_2 + 0 * 64);

    if (unlikely(ret != 0))
        return ret;

    ret = rte_cmp64((const uint8_t *)src_1 + 1 * 64,
            (const uint8_t *)src_2 + 1 * 64);

    if (unlikely(ret != 0))
        return ret;

    ret = rte_cmp64((const uint8_t *)src_1 + 2 * 64,
            (const uint8_t *)src_2 + 2 * 64);

    if (unlikely(ret != 0))
        return ret;

    return rte_cmp64((const uint8_t *)src_1 + 3 * 64,
            (const uint8_t *)src_2 + 3 * 64);
}

/**
 * Compare bytes between two locations. The locations must not overlap.
 *
 * @param src_1
 *   Pointer to the first source of the data.
 * @param src_2
 *   Pointer to the second source of the data.
 * @param n
 *   Number of bytes to compare.
 * @return
 *   zero if src_1 equal src_2
 *   -ve if src_1 less than src_2
 *   +ve if src_1 greater than src_2
 */
static inline int
rte_memcmp(const void *_src_1, const void *_src_2, size_t n)
{
    const uint8_t *src_1 = (const uint8_t *)_src_1;
    const uint8_t *src_2 = (const uint8_t *)_src_2;
    int ret = 0;

    if (n < 16)
        return rte_memcmp_regular(src_1, src_2, n);

    if (n <= 32) {
        ret = rte_cmp16(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

    if (n <= 48) {
        ret = rte_cmp32(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

    if (n <= 64) {
        ret = rte_cmp32(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp16(src_1 + 32, src_2 + 32);

        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

    if (n <= 96) {
        ret = rte_cmp64(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp16(src_1 + 64, src_2 + 64);
        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

    if (n <= 128) {
        ret = rte_cmp64(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp32(src_1 + 64, src_2 + 64);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp16(src_1 + 96, src_2 + 96);
        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

CMP_BLOCK_LESS_THAN_512:
    if (n <= 512) {
        if (n >= 256) {
            ret = rte_cmp256(src_1, src_2);
            if (unlikely(ret != 0))
                return ret;
            src_1 = src_1 + 256;
            src_2 = src_2 + 256;
            n -= 256;
        }
        if (n >= 128) {
            ret = rte_cmp128(src_1, src_2);
            if (unlikely(ret != 0))
                return ret;
            src_1 = src_1 + 128;
            src_2 = src_2 + 128;
            n -= 128;
        }
        if (n >= 64) {
            n -= 64;
            ret = rte_cmp64(src_1, src_2);
            if (unlikely(ret != 0))
                return ret;
            src_1 = src_1 + 64;
            src_2 = src_2 + 64;
        }
        if (n > 32) {
            ret = rte_cmp32(src_1, src_2);
            if (unlikely(ret != 0))
                return ret;
            ret = rte_cmp32(src_1 - 32 + n, src_2 - 32 + n);
            return ret;
        }
        if (n > 0)
            ret = rte_cmp32(src_1 - 32 + n, src_2 - 32 + n);

        return ret;
    }

    while (n > 512) {
        ret = rte_cmp256(src_1 + 0 * 256, src_2 + 0 * 256);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp256(src_1 + 1 * 256, src_2 + 1 * 256);
        if (unlikely(ret != 0))
            return ret;

        src_1 = src_1 + 512;
        src_2 = src_2 + 512;
        n -= 512;
    }
    goto CMP_BLOCK_LESS_THAN_512;
}


#ifdef __cplusplus
}
#endif

#endif /* _RTE_MEMCPY_X86_64_H_ */
