#! /usr/bin/python
# by pts@fazekas.hu at Fri Nov 29 19:20:15 CET 2013

"""Newline-separated file line bisection algorithms."""


def bisect_right(a, x, lo=0, hi=None):
  """Return the index where to insert item x in list a, assuming a is sorted.

  Similar to bisect.bisect_right.

  The return value i is such that all e in a[:i] have e <= x, and all e in
  a[i:] have e > x.  So if x already appears in the list, a.insert(x) will
  insert just after the rightmost x already there.

  Optional args lo (default 0) and hi (default len(a)) bound the
  slice of a to be searched.
  """

  if lo < 0:
    raise ValueError('lo must be non-negative')
  if hi is None:
    hi = len(a)
  while lo < hi:
    mid = (lo + hi) >> 1
    if x < a[mid]:
      hi = mid
    else:
      lo = mid + 1
  return lo


def bisect_left(a, x, lo=0, hi=None):
  """Return the index where to insert item x in list a, assuming a is sorted.

  Similar to bisect.bisect_left.

  The return value i is such that all e in a[:i] have e < x, and all e in
  a[i:] have e >= x.  So if x already appears in the list, a.insert(x) will
  insert just before the leftmost x already there.

  Optional args lo (default 0) and hi (default len(a)) bound the
  slice of a to be searched.
  """

  if lo < 0:
    raise ValueError('lo must be non-negative')
  if hi is None:
    hi = len(a)
  while lo < hi:
    mid = (lo + hi) // 2
    if a[mid] < x:
      lo = mid + 1
    else:
      hi = mid
  return lo


def test():
  a = [10, 20, 30, 30, 30, 30, 30, 40]
  start = bisect_left(a, 30)
  end = bisect_right(a, 30)
  assert a[start : end] == [30] * 5


if __name__ == '__main__':
  test()
