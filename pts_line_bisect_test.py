#! /usr/bin/python
# by pts@fazekas.hu at Fri Nov 29 19:20:15 CET 2013

"""Unit tests for Newline-separated file line bisection algorithms."""

import cStringIO
import unittest

import pts_line_bisect


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