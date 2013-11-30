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
void yfopen(yfile *yf, const char *pathname, off_t size) {
  int fd = open(pathname, O_RDONLY);
  if (fd < 0) {
    fprintf(stderr, "error: open %s: %s\n", pathname, strerror(errno));
    exit(2);
  }
  if (size == -1) {
    size = lseek(fd, 0, SEEK_END);
    if (size + 1U == 0U) {
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

void yfclose(yfile *yf) {
  if (yf->fd >= 0) {
    close(yf->fd);
    yf->fd = -1;
  }
  yf->p = yf->rend = yf->rbuf + YF_READ_BUF_SIZE + 1;
  yf->size = 0;
  yf->ofs = -(YF_READ_BUF_SIZE + 1);  /* So yftell(f) would return 0. */
}

/* Constructor. Opens a file which always returns EOF. */ 
void yfopen_devnull(yfile *yf) {
  yf->fd = -1;
  yfclose(yf);
  *yf->p = '\0';
  yf->p[-1] = '\0';
}

off_t yfgetsize(yfile *yf) {
  return yf->size;
}

off_t yftell(yfile *yf) {
  return yf->p - yf->rbuf + yf->ofs;
}

void yfseek_set(yfile *yf, off_t ofs) {
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
int yfgetc(yfile *yf) {
  if (yf->p == yf->rend) {
    off_t a = yf->p - yf->rbuf + yf->ofs, b;  /* a = yftell(yf); */
    int got, need;
    if (a + 0ULL >= yf->size + 0ULL) return -1;  /* EOF. */
    /* YF_READ_BUF_SIZE must be a power of 2. */
    b = a & -YF_READ_BUF_SIZE;
    yf->p = a - b + yf->rbuf;
    if (yf->ofs != b) {
      a = lseek(yf->fd, b, SEEK_SET);
      if (a + 1U == 0U) {
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
ybool compare_line(yfile *yf, off_t fofs,
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

off_t get_fofs(yfile *yf, off_t ofs) {
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
