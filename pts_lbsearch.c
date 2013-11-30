#define DUMMY \
  set -ex; ${CC:-gcc} -W -Wall -s -O2 -o pts_lbsearch "$0"; : OK; exit
/*
 * pts_lbsearch.c: Fast binary search in a line-sorted file.
 * by pts@fazekas.hu at Sat Nov 30 02:42:03 CET 2013
 *
 * License: GNU GPL v2 or newer, at your choice.
 *
 * Nice properties of this implementation:
 *
 * * no dynamic memory allocation (except possibly for stdio.h)
 * * no unnecessary lseek(2) or read(2) system calls
 * * no unnecessary comparisons for long strings
 * * very small memory usage: only a dozen of offsets of flags in addition to
 *   a single file read buffer (of 8K by default)
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

#ifndef STATIC
#define STATIC static
#endif

typedef char ybool;

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

struct AssertYfReadBufSizeIsPowerOf2 {
  int  AssertYfReadBufSizeIsPowerOf2 :
      (YF_READ_BUF_SIZE & (YF_READ_BUF_SIZE - 1)) == 0;
};

struct AssertYfReadBufSizeIsSmall {
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
      if (errno == ESPIPE) {
        fprintf(stderr, "error: input not seekable, cannot binary search\n");
      } else {
        fprintf(stderr, "error: lseek end: %s\n", strerror(errno));
      }
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
          fprintf(stderr, "error: input not seekable, cannot binary search.\n");
        } else {
          fprintf(stderr, "error: lseek set: %s\n", strerror(errno));
        }
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

/**
 * If the read buffer is empty, read from yf to the read buffer. Then return
 * a slice of the read buffer (buf_out[:result]) without skipping over it.
 * Returns 0 on EOF. The caller can skip through it by calling
 * yfseek_cur(yf, result) later.
 */
int yfpeek(yfile *yf, off_t len, const char **buf_out) {
  int available;
  if (len <= 0) return 0;
  available = yf->rend - yf->p;  // !! fit int
  if (available <= 0 && yfgetc(yf) >= 0) {
    --yf->p;  /* YFUNGET(yf). */
    available = yf->rend - yf->p;
  }
  *buf_out = yf->p;
  return len + 0ULL > available + 0ULL ? available : (int)len;
}

/* --- Bisection (binary search) */

typedef enum compare_mode_t {
  CM_LE,  /* True iff x <= y (where y is read from the file. */
  CM_LT,  /* True iff x < y. */
  CM_LP,  /* x* < y, where x* is x + a fake byte 256 and the end. */
  CM_UNSET,  /* Not set yet. Most functions do not support it. */
} compare_mode_t;

/* Compare x[:xsize] with a line read from yf. */
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

struct cache_entry {
  off_t ofs;
  off_t fofs;
  ybool cmp_result;
};

struct cache {
  struct cache_entry e[2];
  /* 0: 0,1 are used, 0 is active;
   * 1: 0,1 are used, 1 is active;
   * 2: 0 is used, 0 is active;
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

/* x[:xsize] must not contain '\n'. */
STATIC off_t bisect_way(
    yfile *yf, struct cache *cache, off_t lo, off_t hi,
    const char *x, size_t xsize, compare_mode_t cm) {
  const off_t size = yfgetsize(yf);
  off_t mid, midf;
  const struct cache_entry *entry;
  if (hi + 0ULL > size + 0ULL) hi = size;  /* Also applies to hi == -1. */
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

/* x[:xsize] and y[:ysize] must not contain '\n'. */
STATIC void bisect_interval(
    yfile *yf, off_t lo, off_t hi, compare_mode_t cm,
    const char *x, size_t xsize,
    const char *y, size_t ysize,
    off_t *start_out, off_t *end_out) {
  off_t start;
  struct cache cache;
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

STATIC void usage(const char *argv0) {
  fprintf(stderr,
          "Binary search (bisection) in a sorted text file\n"
          "Usage: %s -<flags> <sorted-text-file> <key-x> [<key-y>]\n"
          "<key-x> is the first key to search for\n"
          "<key-y> is the last key to search for; default is <key-x>\n"
          "Flags:\n"
          "e: do bisect_left, open interval (but beginning is always closed)\n"
          "t: do bisect_right, closed interval\n"
          "p: do prefix search\n"
          "c: print file contents (default)\n"
          "o: print file offsets\n"
          "q: don't print anything, just detect if there is a match\n", argv0);
}

STATIC void usage_error(const char *argv0, const char *msg) {
  usage(argv0);
  fprintf(stderr, "usage error: %s\n", msg);
  exit(1);
}

STATIC void print_range(yfile *yf, off_t start, off_t end) {
  int need, got;
  const char *buf;
  if (start >= end) return;
  yfseek_set(yf, start);
  end -= start;
  fflush(stdout);
  while ((need = yfpeek(yf, end, &buf)) > 0) {
    if ((got = write(STDOUT_FILENO, buf, need)) != need) {
      if (got < 0) {
        fprintf(stderr, "error: write stdout: %s\n", strerror(errno));
      } else {
        fprintf(stderr, "error: short write\n");
      }
      exit(2);
    }
    yfseek_cur(yf, need);
    end -= need;
  }
  /* \n is not printed at EOF if there isn't any. */
}

typedef enum printing_t {
  PR_OFFSETS,
  PR_CONTENTS,
  PR_DETECT,
  PR_UNSET,
} printing_t;

int main(int argc, char **argv) {
  yfile yff, *yf = &yff;
  const char *x;
  const char *y;
  const char *filename;
  const char *flags;
  const char *p;
  char flag;
  compare_mode_t cm = CM_UNSET;
  size_t xsize, ysize;
  off_t start, end;
  printing_t printing = PR_UNSET;

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
    } else if (flag == 'o') {
      if (printing != PR_UNSET) usage_error(argv[0], "multiple printing flags");
      printing = PR_OFFSETS;
    } else if (flag == 'c') {
      if (printing != PR_UNSET) usage_error(argv[0], "multiple printing flags");
      printing = PR_CONTENTS;
    } else if (flag == 'q') {
      if (printing != PR_UNSET) usage_error(argv[0], "multiple printing flags");
      printing = PR_DETECT;
    }
  }
  if (cm == CM_UNSET) usage_error(argv[0], "missing boundary flag");
  if (printing == PR_UNSET) printing = PR_CONTENTS;
  if (!y && printing != PR_OFFSETS && cm == CM_LE) {
    usage_error(argv[0], "single-key contents is always empty");
  }
  yfopen(yf, filename, (off_t)-1);
  if (!y && cm == CM_LE && printing == PR_OFFSETS) {
    struct cache cache;
    cache_init(&cache);
    start = bisect_way(yf, &cache, 0, (off_t)-1, x, xsize, cm);  /* CM_LE. */
    yfclose(yf);
    printf("%lld\n", (long long)start);
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
      printf("%lld %lld\n", (long long)start, (long long)end);
    }
    yfclose(yf);
    if (start >= end) exit(3);  /* No match found. */
  }
  if (ferror(stdout)) {
    fprintf(stderr, "error: error writing lbsearch output\n");
    exit(2);
  }
  return 0;
}
