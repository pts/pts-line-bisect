#! /bin/sh
# by pts@fazekas.hu at Sun Dec  1 12:46:54 CET 2013

""":" # Unit tests for newline-separated file line bisection algorithms.

type -p python2.7 >/dev/null 2>&1 && exec python2.7 -- "$0" ${1+"$@"}
type -p python2.6 >/dev/null 2>&1 && exec python2.6 -- "$0" ${1+"$@"}
type -p python2.5 >/dev/null 2>&1 && exec python2.5 -- "$0" ${1+"$@"}
type -p python2.4 >/dev/null 2>&1 && exec python2.4 -- "$0" ${1+"$@"}
exec python -- "$0" ${1+"$@"}; exit 1
"""

import cStringIO
import unittest

import pts_line_bisect


def mini_bisect_left(f, x):
  """Small and slow implementation of bisect_left without size limit."""
  x = x.rstrip('\n')
  if not x: return 0  # Shortcut.
  f.seek(0, 2)  # Seek to EOF.
  size = f.tell()
  if size <= 0: return 0  # Shortcut.
  lo, hi, mid = 0, size - 1, 1
  while lo < hi:
    mid = (lo + hi) >> 1
    if mid > 0:
      f.seek(mid - 1)  # Just to figure out where our line starts.
      f.readline()  # Ignore previous line, find our line.
      midf = f.tell()
    else:
      midf = 0
      f.seek(midf)
    line = f.readline()  # We read at f.tell() == midf.
    # EOF (`not line') is always larger than any line we search for.
    if not line or x <= line.rstrip('\n'):
      hi = mid
    else:
      lo = mid + 1
  if mid == lo: return midf  # Shortcut.
  if lo <= 0: return 0
  f.seek(lo - 1)
  f.readline()
  return f.tell()


def new_bisect_way(f, x, is_left, size=None):
  fofs = old_bisect_way(f, x, is_left, size)
  if is_left and (size is None or getattr(f, 'getvalue', None)):
    if size is not None:
      f = cStringIO.StringIO(f.getvalue()[:size])
    fofs2 = mini_bisect_left(f, x)
    assert fofs == fofs2, (
        'bisect_left mismatch for x=%r: old=%d mini=%s' % (x, fofs, fofs2))
  return fofs


old_bisect_way = pts_line_bisect.bisect_way
pts_line_bisect.bisect_way = new_bisect_way


class PtsLineBisect0Test(unittest.TestCase):
  EXTRA_LEN = 0

  def setUp(self):
    self.f = cStringIO.StringIO(
        '10ten\n20twenty\n30\n30\n30\n30\n30\n40forty' + 'z' * self.EXTRA_LEN)
    self.size = len(self.f.getvalue()) - self.EXTRA_LEN
    if self.EXTRA_LEN:
      self.xsize = self.size
    else:
      self.xsize = None

  def bi(self, x, y=None, is_open=False):
    start, end = pts_line_bisect.bisect_interval(
        self.f, x, y, is_open, self.size)
    assert 0 <= start <= end <= self.size, (start, end, self.size)
    data = self.f.getvalue()[:self.size]
    if start == end:
      return '-' + data[start : start + 5]
    return data[start : end]

  def testBisectLeft(self):
    bisect_left = pts_line_bisect.bisect_left
    self.assertEqual(bisect_left(self.f, '30', self.size), 15)
    self.assertEqual(bisect_left(self.f, '30', self.xsize), 15)
    self.assertEqual(bisect_left(self.f, '32', self.size), 30)
    self.assertEqual(bisect_left(self.f, '32', self.xsize), 30)

  def testBisectRight(self):
    bisect_right = pts_line_bisect.bisect_right
    self.assertEqual(bisect_right(self.f, '30', self.size), 30)
    self.assertEqual(bisect_right(self.f, '30', self.xsize), 30)
    self.assertEqual(bisect_right(self.f, '32', self.size), 30)
    self.assertEqual(bisect_right(self.f, '32', self.xsize), 30)

  def testBisectWay(self):
    bisect_way = pts_line_bisect.bisect_way
    self.assertEqual(bisect_way(self.f, '30', True, self.size), 15)
    self.assertEqual(bisect_way(self.f, '30', True, self.xsize), 15)
    self.assertEqual(bisect_way(self.f, '32', True, self.size), 30)
    self.assertEqual(bisect_way(self.f, '32', True, self.xsize), 30)
    self.assertEqual(bisect_way(self.f, '30', False, self.size), 30)
    self.assertEqual(bisect_way(self.f, '30', False, self.xsize), 30)
    self.assertEqual(bisect_way(self.f, '32', False, self.size), 30)
    self.assertEqual(bisect_way(self.f, '32', False, self.xsize), 30)

  def testBisectInterval(self):
    self.assertEqual(self.bi('30'), '30\n30\n30\n30\n30\n')
    self.assertEqual(self.bi('30', is_open=True), '-30\n30')
    self.assertEqual(self.bi('31'), '-40for')
    self.assertEqual(self.bi('31', is_open=True), '-40for')
    self.assertEqual(self.bi('4'), '-40for')
    self.assertEqual(self.bi('4', is_open=True), '-40for')
    self.assertEqual(self.bi('40'), '-40for')
    self.assertEqual(self.bi('40', is_open=True), '-40for')
    self.assertEqual(self.bi('41'), '-')
    self.assertEqual(self.bi('41', is_open=True), '-')
    self.assertEqual(self.bi('25'), '-30\n30')
    self.assertEqual(self.bi('25', is_open=True), '-30\n30')
    self.assertEqual(self.bi('15'), '-20twe')
    self.assertEqual(self.bi('15', is_open=True), '-20twe')
    self.assertEqual(self.bi('1'), '-10ten')
    self.assertEqual(self.bi('1', is_open=True), '-10ten')
    self.assertEqual(self.bi(''), '-10ten')
    self.assertEqual(self.bi('', is_open=True), '-10ten')
    self.assertEqual(self.bi('10ten'), '10ten\n')
    self.assertEqual(self.bi('10ten', is_open=True), '-10ten')
    self.assertEqual(self.bi('10ten\n\n\n'), '10ten\n')
    self.assertEqual(self.bi('10', '20'), '10ten\n')
    self.assertEqual(self.bi('10', '20', True), '10ten\n')
    self.assertEqual(self.bi('10', '20twenty'), '10ten\n20twenty\n')
    self.assertEqual(self.bi('10', '20twenty', is_open=True), '10ten\n')
    self.assertEqual(
        self.bi('10', '30'), '10ten\n20twenty\n30\n30\n30\n30\n30\n')
    self.assertEqual(self.bi('10', '30', True), '10ten\n20twenty\n')


class PtsLineBisect1Test(PtsLineBisect0Test):
  EXTRA_LEN = 1


class PtsLineBisect2Test(PtsLineBisect0Test):
  EXTRA_LEN = 2


class PtsLineBisect5Test(PtsLineBisect0Test):
  EXTRA_LEN = 5


class PtsLineBisect42Test(PtsLineBisect0Test):
  EXTRA_LEN = 42


if __name__ == '__main__':
  unittest.main()
