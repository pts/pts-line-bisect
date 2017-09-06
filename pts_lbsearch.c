#define DUMMY \
  set -ex; ${CC:-gcc} -ansi -W -Wall -Wextra -Werror=missing-declarations \
      -s -O2 -DNDEBUG -o pts_lbsearch "$0"; : OK; exit
/*
 * pts_lbsearch.c: Fast binary search in a line-sorted text file.
 * by pts@fazekas.hu at Sat Nov 30 02:42:03 CET 2013
 *
 * License: GNU GPL v2 or newer, at your choice.
 *
 * Please note that ordering is the lexicographical order of the byte
 * strings within the input text file, and the byte 10 (LF, '\n') is used as
 * terminator (no CR, \r). If the input file is not sorted, pts_lbsearch.c
 * won't crash, but the results will be incorrect. On Unix, use
 * `LC_CTYPE=C sort <file >file.sorted' to sort files. Without LC_CTYPE=C,
 * sort will use the locale's sort order, which may not be lexicographical if
 * there are non-ASCII characters in the file.
 *
 * The line buffering code in the binary search implementation in this file
 * is very tricky. See (ARTICLE)
 * http://pts.github.io/pts-line-bisect/line_bisect_evolution.html for a
 * detailed explananation, containing the design and analysis of the
 * algorithms implemented in this file.
 *
 * Nice properties of this implementation:
 *
 * * no dynamic memory allocation (except possibly for stdio.h)
 * * no unnecessary lseek(2) or read(2) system calls
 * * no unnecessary comparisons for long strings
 * * very small memory usage: only a few dozen of offsets and flags in addition
 *   to a single file read buffer (of 8K by default)
 * * no printf
 * * compiles without warnings in C and C++
 *   (gcc -std=c89; gcc -std=c99; gcc -std=c11;
 *   gcc -ansi; g++ -std=c++98; g++ -std=c++11; g++ -std=ansi; also
 *   correspondingly with clang and clang++).
 *
 * -Werror=implicit-function-declaration is not supported by gcc-4.1.
 *
 * TODO(pts): Add flag `-aq' for only CM_LT offset.
 */

/* #define _LARGEFILE64_SOURCE  -- this would enable off64_t, lseek64 etc. */
#ifndef _FILE_OFFSET_BITS
/* Manual testing with a file of 6.2GB worked. */
/* TODO(pts): What is the Win32 (MinGW) equivalent? */
#define _FILE_OFFSET_BITS 64
#endif

#ifdef __XTINY__
#include <xtiny.h>
#undef  assert
#define assert(x)
#undef  strerror
#define strerror(errno) "(errno)"
#ifndef __XTINY_OFF_T_IS_64_BITS__
/* In case <xtiny.h> doesn't support _FILE_OFFSET_BITS. */
#define off_t off64_t
#define lseek lseek64
#endif
#else
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>  /* Not strictly needed. */
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#endif

/* Win32 compatibility */
/* TODO(pts): Verify that it works on Win32. */
#ifndef O_BINARY
#define O_BINARY 0
#endif

#ifndef STATIC
#define STATIC static
#endif

typedef char ybool;

#define YF_READ_BUF_SIZE 8192  /* Must be a power of 2. */

/* --- Buffered, seekable file reader.
 *
 * We implement our own optimized buffered file reader, which makes sure that
 * there are no unnecessary lseek(2) or read(2) system calls, not even when
 * a combination of read and seek operations are issued.
 */

#ifndef YF_READ_BUF_SIZE
/* Sensible values are 4096, 8192, 16384 and 32768.
 * Larger values most probably don't make the program measurably faster.
 */
#define YF_READ_BUF_SIZE 8192
#endif

struct AssertYfReadBufSizeIsPowerOf2_Struct {
  int  AssertYfReadBufSizeIsPowerOf2 :
      (YF_READ_BUF_SIZE & (YF_READ_BUF_SIZE - 1)) == 0;
};

struct AssertYfReadBufSizeIsSmall_Struct {
  int  AssertYfReadBufSizeIsSmall :
      (((unsigned)YF_READ_BUF_SIZE << 2) >> 2) + 0ULL ==
      YF_READ_BUF_SIZE + 0ULL;
};

typedef struct yfile {
  char *p;
  /* Invariant: *yf->rend == '\0'. */
  char *rend;
  int fd;
  off_t ofs;  /* File offset at the beginning of rbuf. */
  off_t size;
  char rbuf[YF_READ_BUF_SIZE + 2];
} yfile;

STATIC __attribute__((noreturn)) void die5_code(
    const char *msg1, const char *msg2, const char *msg3, const char *msg4,
    const char *msg5, int exit_code) {
  const size_t msg1_size = strlen(msg1), msg2_size = strlen(msg2);
  const size_t msg3_size = strlen(msg3), msg4_size = strlen(msg4);
  const size_t msg5_size = strlen(msg5);  /* !! */
  (void)!write(STDERR_FILENO, msg1, msg1_size);
  (void)!write(STDERR_FILENO, msg2, msg2_size);
  (void)!write(STDERR_FILENO, msg3, msg3_size);
  (void)!write(STDERR_FILENO, msg4, msg4_size);
  (void)!write(STDERR_FILENO, msg5, msg5_size);
  exit(exit_code);
}

STATIC __attribute__((noreturn)) void die2_strerror(
    const char *msg1, const char *msg2) {
  die5_code(msg1, msg2, ": ", strerror(errno), "\n", 2);
}

STATIC __attribute__((noreturn)) void die1(const char *msg1) {
  die5_code(msg1, "", "", "", "\n", 2);
}

/** Constructor. Opens and initializes yf.
 * If size != (off_t)-1, then it will be imposed as a limit.
 */
STATIC void yfopen(yfile *yf, const char *pathname, off_t size) {
  int fd = open(pathname, O_RDONLY | O_BINARY, 0);
  if (fd < 0) {
    die2_strerror("error: open ", pathname);
    exit(2);
  }
  if (size == -1) {
    size = lseek(fd, 0, SEEK_END);
    if (size + 1ULL == 0ULL) {
      if (errno == ESPIPE) {
        die1("error: input not seekable, cannot binary search");
      } else {
        die2_strerror("error: lseek end", "");
      }
    }
  }
  yf->p = yf->rend = yf->rbuf + YF_READ_BUF_SIZE + 1;
  *yf->p = '\0';
  yf->p[-1] = '\0';
  yf->fd = fd;
  yf->size = size;
  yf->ofs = -(YF_READ_BUF_SIZE + 1);  /* So yftell(f) would return 0. */
}

STATIC void yfclose(yfile *yf) {
  if (yf->fd >= 0) {
    close(yf->fd);
    yf->fd = -1;
  }
  yf->p = yf->rend = yf->rbuf + YF_READ_BUF_SIZE + 1;
  yf->size = 0;
  yf->ofs = -(YF_READ_BUF_SIZE + 1);  /* So yftell(f) would return 0. */
}

#if 0
/* Constructor. Opens a file which always returns EOF. */
STATIC void yfopen_devnull(yfile *yf) {
  yf->fd = -1;
  yfclose(yf);
  *yf->p = '\0';
  yf->p[-1] = '\0';
}
#endif

STATIC off_t yfgetsize(yfile *yf) {
  return yf->size;
}

#if 0
STATIC off_t yftell(yfile *yf) {
  return yf->p - yf->rbuf + yf->ofs;
}
#endif

STATIC off_t yflimit(yfile *yf, off_t size) {
  off_t ofs;
  if (size + 0ULL < yf->size + 0ULL) {
    yf->size = size;
    /* Fix up yf->p and yf->rend if they are too large. */
    if (yf->rend - yf->rbuf + yf->ofs + 0ULL > yf->size + 0ULL &&
        yf->p != yf->rbuf + YF_READ_BUF_SIZE + 1) {
      if (yf->p - yf->rbuf + yf->ofs + 0ULL > yf->size + 0ULL) {
        /* TODO(pts): Do it without dropping all the caches. */
        ofs = yf->p - yf->rbuf + yf->ofs;
        yf->p = yf->rend = yf->rbuf + YF_READ_BUF_SIZE + 1;
        yf->ofs = ofs - (YF_READ_BUF_SIZE + 1);
      } else {
        yf->rend = yf->size - yf->ofs + yf->rbuf;  /* Make it smaller. */
        *yf->rend = '\0';
      }
    }
  }
  return yf->size;
}

/* It's possible to seek beyond the file size. */
STATIC void yfseek_set(yfile *yf, off_t ofs) {
  char * const rbuf1 = yf->rbuf + YF_READ_BUF_SIZE + 1;
  assert(ofs >= 0);
  /* TODO(pts): Convert off_t to its unsigned equivalent? + 0U doesn't seem to
   * make a difference. + 0ULL seems to solve it.
   */
  if (yf->p != rbuf1 && ofs - yf->ofs + 0ULL <= yf->rend - yf->rbuf + 0ULL) {
    yf->p = ofs - yf->ofs + yf->rbuf;
  } else {  /* Forget about the cached read buffer. */
    yf->p = yf->rend = rbuf1;
    yf->ofs = ofs - (YF_READ_BUF_SIZE + 1);
  }
}

STATIC void yfseek_cur(yfile *yf, off_t ofs) {
  if (ofs + 0ULL <= yf->rend - yf->p + 0ULL) {  /* Shortcut for ofs >= 0. */
    yf->p += ofs;
  } else {
    yfseek_set(yf, yf->p - yf->rbuf + yf->ofs + ofs);
  }
}

/** Fast macro for yfgetc. */
#define YFGETCHAR(yf) (*(yf)->p == '\0' ? yfgetc(yf) : \
    (int)*(unsigned char*)(yf)->p++)

/** Can only be called after a getchar returning non-EOF. */
#define YFUNGET(yf) ((void)--(yf)->p)

/** Returns -1 on EOF, or 0..255. */
STATIC int yfgetc(yfile *yf) {
  if (yf->p == yf->rend) {
    off_t a = yf->p - yf->rbuf + yf->ofs, b;  /* a = yftell(yf); */
    int got, need;
    if (a + 0ULL >= yf->size + 0ULL) return -1;  /* EOF. */
    /* YF_READ_BUF_SIZE must be a power of 2 for this below. */
    b = a & -YF_READ_BUF_SIZE;
    yf->p = a - b + yf->rbuf;
    if (yf->ofs != b) {
      a = lseek(yf->fd, b, SEEK_SET);
      if (a + 1ULL == 0ULL) {
        if (errno == ESPIPE) {
          die1("error: input not seekable, cannot binary search");
        } else {
          die2_strerror("error: lseek set", "");  /* !! merge */
        }
        exit(2);
      }
      if (a != b) {  /* Should not happen. */
        die2_strerror("error: lseek set offset", "");
        exit(2);
      }
      yf->ofs = b;
    }
    need = b + YF_READ_BUF_SIZE + 0ULL > yf->size + 0ULL ?
        yf->size - b : YF_READ_BUF_SIZE;
    got = yf->fd < 0 ? 0 : read(yf->fd, yf->rbuf, need);
    if (got < 0) {
      die2_strerror("error: read", "");
    }
    *(yf->rend = yf->rbuf + got) = '\0';
    b += got;
    if (got < need && b + 0ULL < yf->size + 0ULL) {
      yf->size = b;
    }
    if (b + 0ULL <= a + 0ULL) {  /* yf->p is past the buffer. */
      yf->p = yf->rend;
      return -1;  /* EOF. */
    }
  }
  return *(unsigned char*)yf->p++;
}

/* If len <= 0 or at EOF, just returns 0. Otherwise, it makes sure that the
 * read buffer of yf contains it least 1 byte available (by calling
 * yfgetc(yf) if needed), and returns min(available, len), thus the return
 * value is at least 1. Also sets *buf_out so that (*buf_out[:result]) is
 * the next available bytes with the read buffer. It doesn't skip over these
 * bytes though, the caller can do it by yfseek_cur(yf, result) later.
 */
STATIC int yfpeek(yfile *yf, off_t len, const char **buf_out) {
  int available;
  if (len <= 0) return 0;
  available = yf->rend - yf->p;  /* This fits to an int. */
  if (available <= 0 && yfgetc(yf) >= 0) {
    --yf->p;  /* YFUNGET(yf). */
    available = yf->rend - yf->p;
  }
  *buf_out = yf->p;
  return len + 0ULL > available + 0ULL ? available : (int)len;
}

/* --- Bisection (binary search)
 *
 * The algorithms and data structures below are complex, tricky, and very
 * underdocumented. See (ARTICLE) above for a detailed explanation of both
 * design and implementation.
 */

/* Returns the file offset of the line starting at ofs, or if no line
 * starts their, then the the offset of the next line.
 */
STATIC off_t get_fofs(yfile *yf, off_t ofs) {
  int c;
  off_t size;
  assert(ofs >= 0);
  if (ofs == 0) return 0;
  size = yfgetsize(yf);
  if (ofs > size) return size;
  --ofs;
  yfseek_set(yf, ofs);
  for (;;) {
    if ((c = YFGETCHAR(yf)) < 0) return ofs;
    ++ofs;
    if (c == '\n') return ofs;
  }
}

typedef enum compare_mode_t {
  CM_LE,  /* True iff x <= y (where y is read from the file). */
  CM_LT,  /* True iff x < y. */
  CM_LP,  /* x* < y, where x* is x + a fake byte 256 and the end. */
  CM_UNSET,  /* Not set yet. Most functions do not support it. */
} compare_mode_t;

/* Compares x[:xsize] with a line read from yf. */
STATIC ybool compare_line(yfile *yf, off_t fofs,
                          const char *x, size_t xsize, compare_mode_t cm) {
  int b, c;
  yfseek_set(yf, fofs);
  c = YFGETCHAR(yf);
  if (c < 0) return 1;  /* Special casing of EOF at BOL. */
  YFUNGET(yf);
  /* TODO(pts): Possibly speed up the loop with yfpeek(...). */
  for (;;) {
    c = YFGETCHAR(yf);
    if (c < 0 || c == '\n') {
      return cm == CM_LE ? xsize == 0 : 0;
    } else if (xsize == 0) {
      return cm != CM_LP;
    } else if ((b = (int)*(unsigned char*)x) != c) {
      return b < c;
    }
    ++x;
    --xsize;
  }
}

struct cache_entry {
  off_t ofs;
  off_t fofs;
  ybool cmp_result;
};

struct cache {
  struct cache_entry e[2];
  /* 0: 0,1 are used, 0 is active;
   * 1: 0,1 are used, 1 is active;
   * 2: 0 is used and active, 1 is unused;
   * 3: 0,1 are unused.
   */
  int active;
};

#define CACHE_HAS_0(a) ((a) != 3)
#define CACHE_HAS_1(a) ((a) < 2)
#define CACHE_GET_ACTIVE(a) ((a) & 1)  /* Valid only if CACHE_HAS_0(a). */

/** Can be called again to clear the cache. */
STATIC void cache_init(struct cache *cache) {
  cache->active = 3;
}

/* x[:xsize] must not contain '\n'. */
STATIC const struct cache_entry *get_using_cache(
    yfile *yf, struct cache *cache, off_t ofs,
    const char *x, size_t xsize, compare_mode_t cm) {
  int a = cache->active;
  struct cache_entry *entry;
  off_t fofs;
  assert(ofs >= 0);
  /* TODO(pts): Add tests for efficient and correct cache usage. */
  /* TODO(pts): Add tests for code coverage. */
  if (CACHE_HAS_0(a) &&
      cache->e[0].ofs <= ofs && ofs <= cache->e[0].fofs) {
    if (a == 1) cache->active = a = 0;
  } else if (CACHE_HAS_1(a) &&
             cache->e[1].ofs <= ofs && ofs <= cache->e[1].fofs) {
    if (a == 0) cache->active = a = 1;
  } else {
    fofs = get_fofs(yf, ofs);
    assert(ofs <= fofs);
    if (CACHE_HAS_0(a) && cache->e[0].fofs == fofs) {
      if (a == 1) cache->active = a = 0;
      if (cache->e[0].ofs > ofs) cache->e[0].ofs = ofs;
    } else if (CACHE_HAS_1(a) && cache->e[1].fofs == fofs) {
      if (a == 0) cache->active = a = 1;
      if (cache->e[1].ofs > ofs) cache->e[1].ofs = ofs;
    } else {
      if (CACHE_HAS_0(a)) {
        cache->active = a = CACHE_GET_ACTIVE(a) ^ 1;
        entry = cache->e + a;
      } else {
        cache->active = a = 2;
        entry = cache->e;
      }
      /* Fill newly activated cache entry. */
      entry->fofs = fofs;
      entry->ofs = ofs;
      entry->cmp_result = compare_line(yf, fofs, x, xsize, cm);
      return entry;  /* Shortcut, the return below would do the same. */
    }
  }
  return cache->e + CACHE_GET_ACTIVE(a);
}

STATIC off_t get_fofs_using_cache(
    yfile *yf, struct cache *cache, off_t ofs) {
  int a = cache->active;
  off_t fofs;
  assert(ofs >= 0);
  if (ofs == 0) return 0;
  if (CACHE_HAS_0(a) &&
      cache->e[0].ofs <= ofs && ofs <= cache->e[0].fofs) {
    if (a == 1) cache->active = a = 0;
    return cache->e[0].fofs;
  } else if (CACHE_HAS_1(a) &&
             cache->e[1].ofs <= ofs && ofs <= cache->e[1].fofs) {
    if (a == 0) cache->active = a = 1;
    return cache->e[1].fofs;
  } else {
    fofs = get_fofs(yf, ofs);
    assert(ofs <= fofs);
    if (CACHE_HAS_0(a) && cache->e[0].fofs == fofs) {
      if (a == 1) cache->active = a = 0;
      if (cache->e[0].ofs > ofs) cache->e[0].ofs = ofs;
    } else if (CACHE_HAS_1(a) && cache->e[1].fofs == fofs) {
      if (a == 0) cache->active = a = 1;
      if (cache->e[1].ofs > ofs) cache->e[1].ofs = ofs;
    }
    /* We don't update the cache, because we don't know cmp_result, and we
     * are too lazy to compute it.
     */
    return fofs;
  }
}

/* x[:xsize] must not contain '\n'.
 *
 * cm=CM_LE is equivalent to is_left=true and is_open=true.
 * cm=CM_LT is equivalent to is_left=false and is_open=false.
 * cm=CL_LP is also supported, it does prefix search.
 */
STATIC off_t bisect_way(
    yfile *yf, struct cache *cache, off_t lo, off_t hi,
    const char *x, size_t xsize, compare_mode_t cm) {
  const off_t size = yfgetsize(yf);
  off_t mid, midf;
  const struct cache_entry *entry;
  if (hi + 0ULL > size + 0ULL) hi = size;  /* Also applies to hi == -1. */
  if (xsize == 0) {  /* Shortcuts. */
    if (cm == CM_LE) hi = lo;  /* Faster for lo == 0. Returns right below. */
    if (cm == CM_LP && hi == size) return hi;
  }
  if (lo >= hi) return get_fofs_using_cache(yf, cache, lo);
  do {
    mid = (lo + hi) >> 1;
    entry = get_using_cache(yf, cache, mid, x, xsize, cm);
    midf = entry->fofs;
    if (entry->cmp_result) {
      hi = mid;
    } else {
      lo = mid + 1;
    }
  } while (lo < hi);
  return mid == lo ? midf : get_fofs_using_cache(yf, cache, lo);
}

/* x[:xsize] and y[:ysize] must not contain '\n'. */
STATIC void bisect_interval(
    yfile *yf, off_t lo, off_t hi, compare_mode_t cm,
    const char *x, size_t xsize,
    const char *y, size_t ysize,
    off_t *start_out, off_t *end_out) {
  off_t start;
  struct cache cache;
  /* TODO(pts): If y < x, then don't even read the file. Smart compare! */
  cache_init(&cache);
  *start_out = start = bisect_way(yf, &cache, lo, hi, x, xsize, CM_LE);
  if (cm == CM_LE && xsize == ysize && 0 == memcmp(x, y, xsize)) {
    *end_out = start;
  } else {
    /* Don't use a shared cache, because x or cm are different. */
    cache_init(&cache);
    *end_out = bisect_way(yf, &cache, start, hi, y, ysize, cm);
  }
}

/* --- main */

STATIC __attribute__((noreturn)) void usage_error(
    const char *argv0, const char *msg) {
  die5_code("Binary search (bisection) in a sorted text file\n"
            "Usage: ", argv0, "-<flags> <sorted-text-file> <key-x> [<key-y>]\n"
            "<key-x> is the first key to search for\n"
            "<key-y> is the last key to search for; default is <key-x>\n"
            "Flags:\n"
            "e: do bisect_left, open interval end\n"
            "t: do bisect_right, closed interval end\n"
            "b: do bisect_left for interval start (default)\n"
            "a: do bisect_right for interval start (for append position)\n"
            "p: do prefix search\n"
            "c: print file contents (default)\n"
            "o: print file offsets\n"
            "q: don't print anything, just detect if there is a match\n"
            "i: ignore incomplete last line (may be appended to right now)\n"
            "usage error: ", msg, "\n",
            1);
}

STATIC void write_all_to_stdout(const char *buf, size_t size) {
  size_t got = write(STDOUT_FILENO, buf, size);
  if (got == size) {
  } else if (got + 1U == 0U) {
   die2_strerror("error: write stdout", "");
  } else {
   die1("error: short write");
  }
}

STATIC void print_range(yfile *yf, off_t start, off_t end) {
  int need;
  const char *buf;
  if (start >= end) return;
  yfseek_set(yf, start);
  end -= start;
#if defined(__MSDOS__) || defined(_WIN32) || defined(_WIN64)
  /* _WIN32 and _WIN64 cover __CYGWIN__, __MINGW32__, __MINGW64__ and
   * _MSC_VER > 1000, no need to check for more.
   */
  setmode(STDOUT_FILENO, O_BINARY);
#endif
  while ((need = yfpeek(yf, end, &buf)) > 0) {
    write_all_to_stdout(buf, need);
    yfseek_cur(yf, need);
    end -= need;
  }
  /* \n is not printed at EOF if there isn't any. */
#if defined(__MSDOS__) || defined(_WIN32) || defined(_WIN64)
  /* _WIN32 and _WIN64 cover __CYGWIN__, __MINGW32__, __MINGW64__ and
   * _MSC_VER > 1000, no need to check for more.
   */
  setmode(STDOUT_FILENO, 0);
#endif
}

#if defined(__i386__) && __SIZEOF_INT__ == 4 && __SIZEOF_LONG_LONG__ == 8 && \
    defined(__GNUC__)
/* A smaller implementation of division for format_unsigned, which doesn't
 * call the __udivdi3 (for 64-bit /) and __umoddi3 (for 64-bit %) functions
 * from libgcc. This implementation can be smaller because the divisor (b) is
 * only 32 bits.
 */

typedef unsigned UInt32;
typedef unsigned long long UInt64;
/* Returns *a % b, and sets *a = *a_old / b; */
STATIC __inline__ UInt32 UInt64DivAndGetMod(UInt64 *a, UInt32 b) {
  /* http://stackoverflow.com/a/41982320/97248 */
  UInt32 upper = ((UInt32*)a)[1], r;
  ((UInt32*)a)[1] = 0;
  if (upper >= b) {
    ((UInt32*)a)[1] = upper / b;
    upper %= b;
  }
  __asm__("divl %2" : "=a" (((UInt32*)a)[0]), "=d" (r) :
      "rm" (b), "0" (((UInt32*)a)[0]), "1" (upper));
  return r;
}
/* Returns *a % b, and sets *a = *a_old / b; */
STATIC __inline__ UInt32 UInt32DivAndGetMod(UInt32 *a, UInt32 b) {
  /* gcc-4.4 is smart enough to optimize the / and % to a single divl. */
  const UInt32 r = *a % b;
  *a /= b;
  return r;
}

/** Returns p + size of formatted output. */
STATIC char *format_unsigned(char *p, off_t i) {
  struct AssertOffTSizeIs4or8_Struct {
    int  AssertOffTSizeIs4or8 : sizeof(i) == 4 || sizeof(i) == 8;
  };
  char *q = p, *result, c;
  assert(i >= 0);
  do {
    *q++ = '0' + (
        sizeof(i) == 8 ? UInt64DivAndGetMod((UInt64*)&i, 10) :
        sizeof(i) == 4 ? UInt32DivAndGetMod((UInt32*)&i, 10) :
        0);  /* 0 never happens, see AssertOffTSizeIs4or8 above. */
  } while (i != 0);
  result = q--;
  while (p < q) {  /* Reverse the string between p and q. */
    c = *p; *p++ = *q; *q-- = c;
  }
  return result;
}
#else

/** Returns p + size of formatted output. */
static char *format_unsigned(char *p, off_t i) {
  char *q = p, *result, c;
  assert(i >= 0);
  do {
    *q++ = '0' + (i % 10);
  } while ((i /= 10) != 0);
  result = q--;
  while (p < q) {  /* Reverse the string between p and q. */
    c = *p; *p++ = *q; *q-- = c;
  }
  return result;
}
#endif

typedef enum printing_t {
  PR_OFFSETS,
  PR_CONTENTS,
  PR_DETECT,
  PR_UNSET,
} printing_t;

typedef enum incomplete_t {
  IN_IGNORE,  /* Ignore incomplete last line of file. */
  IN_USE,  /* Use incomplete last line of file as if it had a trailin '\n'. */
  IN_UNSET,  /* Not set yet. Most functions do not support it. */
} incomplete_t;

int main(int argc, char **argv) {
  yfile yff, *yf = &yff;
  const char *x;
  const char *y;
  const char *filename;
  const char *flags;
  const char *p;
  char flag;
  /* Large enough to hold 2 off_t()s and 2 more bytes. */
  char ofsbuf[sizeof(off_t) * 6 + 2], *ofsp;
  compare_mode_t cm = CM_UNSET;
  compare_mode_t cmstart = CM_UNSET;
  size_t xsize, ysize;
  off_t start, end;
  printing_t printing = PR_UNSET;
  incomplete_t incomplete = IN_UNSET;

  /* Parse the command-line. */
  if (argc != 4 && argc != 5) usage_error(argv[0], "incorrect argument count");
  if (argv[1][0] != '-') usage_error(argv[0], "missing flags");
  flags = argv[1] + 1;
  filename = argv[2];
  x = argv[3];
  for (p = x; *p && *p != '\n'; ++p) {}
  xsize = p - x;  /* Make sure x[:psize] doesn't contain '\n'. */
  if (argc == 4) {
    y = NULL;
    ysize = 0;
  } else {
    y = argv[4];
    for (p = y; *p && *p != '\n'; ++p) {}
    ysize = p - y;  /* Make sure x[:psize] doesn't contain '\n'. */
  }
  /* TODO(pts): Make the initial lo and hi offsets specifiable. */
  for (p = flags; (flag = *p); ++p) {
    if (flag == 'e') {
      if (cm != CM_UNSET) usage_error(argv[0], "multiple boundary flags");
      cm = CM_LE;
    } else if (flag == 't') {
      if (cm != CM_UNSET) usage_error(argv[0], "multiple boundary flags");
      cm = CM_LT;
    } else if (flag == 'p') {
      if (cm != CM_UNSET) usage_error(argv[0], "multiple boundary flags");
      cm = CM_LP;
    } else if (flag == 'b') {
      if (cmstart != CM_UNSET) usage_error(argv[0], "multiple start flags");
      cmstart = CM_LE;
    } else if (flag == 'a') {
      if (cmstart != CM_UNSET) usage_error(argv[0], "multiple start flags");
      cmstart = CM_LT;
    } else if (flag == 'o') {
      if (printing != PR_UNSET) usage_error(argv[0], "multiple printing flags");
      printing = PR_OFFSETS;
    } else if (flag == 'c') {
      if (printing != PR_UNSET) usage_error(argv[0], "multiple printing flags");
      printing = PR_CONTENTS;
    } else if (flag == 'q') {
      if (printing != PR_UNSET) usage_error(argv[0], "multiple printing flags");
      printing = PR_DETECT;
    } else if (flag == 'i') {
      if (incomplete != IN_UNSET) {
        usage_error(argv[0], "multiple incomplete flags");
      }
      incomplete = IN_IGNORE;
    } else {
      usage_error(argv[0], "unsupported flag");
    }
  }
  if (printing == PR_UNSET) printing = PR_CONTENTS;
  if (incomplete == IN_UNSET) incomplete = IN_USE;
  if (cmstart == CM_UNSET) cmstart = CM_LE;
  if (cm == CM_UNSET) usage_error(argv[0], "missing boundary flag");
  if (cmstart == CM_LT && !(!y && cm == CM_LE && printing == PR_OFFSETS)) {
    /* TODO(pts): Make cmstart=CM_LT work in bisect_interval etc. */
    usage_error(argv[0], "flag -a needs -eo and no <key-y>");
  }
  if (!y && printing != PR_OFFSETS && cm == CM_LE) {
    usage_error(argv[0], "single-key contents is always empty");
  }

  yfopen(yf, filename, (off_t)-1);
  if (incomplete == IN_IGNORE) {
    off_t size = yfgetsize(yf);
    int c;
    while (size != 0) {
      yfseek_set(yf, size - 1);
      if ((c = YFGETCHAR(yf)) < 0 || c == '\n') break;
      --size;
    }
    yflimit(yf, size);
  }
  if (!y && cm == CM_LE && printing == PR_OFFSETS) {
    struct cache cache;
    cache_init(&cache);
    start = bisect_way(yf, &cache, 0, (off_t)-1, x, xsize, cmstart);
    yfclose(yf);
    ofsp = ofsbuf;
    ofsp = format_unsigned(ofsp, start);
    *ofsp++ = '\n';
    write_all_to_stdout(ofsbuf, ofsp - ofsbuf);
  } else if (printing == PR_DETECT &&
             (!y || (xsize == ysize && 0 == memcmp(x, y, xsize)))) {
    /* This branch is just a shortcut, it doesn't change the results. */
    struct cache cache;
    const struct cache_entry *entry;
    /* Shortcut just to detect if x is present. */
    if (cm == CM_LE) exit(3);  /* start:end range would always be empty. */
    cache_init(&cache);
    start = bisect_way(yf, &cache, 0, (off_t)-1, x, xsize, CM_LE);
    cache_init(&cache);  /* Can't reuse cache, cm has changed. */
    /* We don't benefit any speed from the cache here (because it's empty),
     * but we reuse the existing code to compare a single line from yf.
     */
    entry = get_using_cache(yf, &cache, start, x, xsize, cm);
    yfclose(yf);
    if (entry->cmp_result) exit(3);  /* exit(3) iff x not found in yf. */
  } else {
    if (!y) {
      y = x;
      ysize = xsize;
    }
    bisect_interval(yf, 0, (off_t)-1, cm, x, xsize, y, ysize, &start, &end);
    if (printing == PR_CONTENTS) {
      print_range(yf, start, end);
    } else if (printing == PR_OFFSETS) {
      ofsp = ofsbuf;
      ofsp = format_unsigned(ofsp, start);
      *ofsp++ = ' ';
      ofsp = format_unsigned(ofsp, end);
      *ofsp++ = '\n';
      write_all_to_stdout(ofsbuf, ofsp - ofsbuf);
    }
    yfclose(yf);
    if (start >= end) exit(3);  /* No match found. */
  }
  return EXIT_SUCCESS;  /* 0. */
}
