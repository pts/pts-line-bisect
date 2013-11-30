#define DUMMY \
  set -ex; ${CC:-gcc} -W -Wall -s -O2 -o pts_lbsearch "$0"; : OK; exit
/*
 * pts_lbsearch.c: Fast binary search in a line-sorted file.
 * by pts@fazekas.hu at Sat Nov 30 02:42:03 CET 2013
 *
 * TODO(pts): Test largefile support.
 * TODO(pts): Document LC_ALL=C sort etc.
 */

/* #define _LARGEFILE64_SOURCE  -- this would be off64_t, lseek64 etc. */
#define _FILE_OFFSET_BITS 64

#define YF_READ_BUF_SIZE 8192  /* Must be a power of 2. */

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#define STATIC static

typedef char ybool;  /* TODO(pts): Is this needed? */ 

/* --- Buffered, seekable file reader. */

#ifndef YF_READ_BUF_SIZE
#define YF_READ_BUF_SIZE 8192
#endif

struct AssertYfReadBufSizeIsPowerOf2 {
  int _ : (YF_READ_BUF_SIZE & (YF_READ_BUF_SIZE - 1)) == 0;
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

/** Constructor. Opens and initializes yf.
 * If size != (off_t)-1, then it will be imposed as a limit.
 */
STATIC void yfopen(yfile *yf, const char *pathname, off_t size) {
  int fd = open(pathname, O_RDONLY);
  if (fd < 0) {
    fprintf(stderr, "error: open %s: %s\n", pathname, strerror(errno));
    exit(2);
  }
  if (size == -1) {
    size = lseek(fd, 0, SEEK_END);
    if (size + 1ULL == 0ULL) {
      fprintf(stderr, "error: lseek end: %s\n", strerror(errno));
      exit(2);
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
    /* YF_READ_BUF_SIZE must be a power of 2. */
    b = a & -YF_READ_BUF_SIZE;
    yf->p = a - b + yf->rbuf;
    if (yf->ofs != b) {
      a = lseek(yf->fd, b, SEEK_SET);
      if (a + 1ULL == 0ULL) {
        fprintf(stderr, "error: lseek set: %s\n", strerror(errno));
        exit(2);
      }
      if (a != b) {  /* Should not happen. */
        fprintf(stderr, "error: lseek set offset\n");
        exit(2);
      }
      yf->ofs = b;
    }
    need = b + YF_READ_BUF_SIZE + 0ULL > yf->size + 0ULL ?
        yf->size - b : YF_READ_BUF_SIZE; 
    got = yf->fd < 0 ? 0 : read(yf->fd, yf->rbuf, need);
    if (got < 0) {
      fprintf(stderr, "error: read: %s\n", strerror(errno));
      exit(2);
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

/* --- Compare */

typedef enum compare_mode_t {
  CM_LE,  /* True iff x <= y (where y is read from the file. */
  CM_LT,  /* True iff x < y. */
  CM_LP,  /* x* < y, where x* is x + a fake byte 256 and the end. */
} compare_mode_t;

/* Compare x[:xsize] with a line read from yf. */
STATIC ybool compare_line(yfile *yf, off_t fofs,
                          const char *x, size_t xsize, compare_mode_t cm) {
  int b, c;
  yfseek_set(yf, fofs);
  c = YFGETCHAR(yf);
  if (c < 0) return 1;  /* Special casing of EOF at BOL. */
  YFUNGET(yf);
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

enum CacheEntryResult {
  CER_CMP_FALSE = 0,
  CER_CMP_TRUE = 1,
  CER_UNUSED = 2,
};

struct cache_entry {
  off_t ofs;
  off_t fofs;
  ybool cmp_result;
};

struct cache {
  struct cache_entry e[2];
  /* 0: 01 used, 0 is active;
   * 1: 01 used, 1 is active;
   * 2: 0 used, 0 is active;
   * 3: none used.
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

STATIC const struct cache_entry *get_using_cache(
    yfile *yf, struct cache *cache, off_t ofs,
    const char *x, size_t xsize, compare_mode_t cm) {
  int a = cache->active;
  struct cache_entry *entry;
  off_t fofs;
  assert(ofs >= 0);
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

STATIC off_t bisect_way(
    yfile *yf, struct cache *cache, off_t lo, off_t hi,
    const char *x, size_t xsize, compare_mode_t cm) {
  const off_t size = yfgetsize(yf);
  off_t mid, midf;
  const struct cache_entry *entry;
  if (hi + 0ULL > size + 0ULL) hi = size;  /* Also applies to hi == -1. */
  while (xsize > 0 && x[xsize - 1] == '\n') --xsize;
  /* is_left=true, is_open=true correspond to cm=CM_LE */
  if (cm == CM_LE && xsize == 0) return 0;  /* Shortcut. */
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

STATIC void bisect_interval(
    yfile *yf, off_t lo, off_t hi, compare_mode_t cm,
    const char *x, size_t xsize,
    const char *y, size_t ysize,
    off_t *start_out, off_t *end_out) {
  off_t start;
  struct cache cache;
  while (xsize > 0 && x[xsize - 1] == '\n') --xsize;
  while (ysize > 0 && x[ysize - 1] == '\n') --ysize;
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

int main(int argc, char **argv) {
  yfile yff, *yf = &yff;
  int c;
  off_t ofs;
  (void)argc;
  (void)argv;
  yfopen(yf, argv[1], (off_t) -1); 
  for (ofs = yfgetsize(yf); ofs != 0; --ofs) {
    yfseek_set(yf, ofs - 1);
    c = YFGETCHAR(yf);
    assert(c >= 0);
    putchar(c);
  }
  yfclose(yf);
  return 0;
}
