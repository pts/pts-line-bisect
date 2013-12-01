#! /usr/bin/python
# by pts@fazekas.hu at Fri Nov 29 19:20:15 CET 2013

"""Newline-separated file line bisection algorithms."""

def _get_using_cache(ab, ofs, size, fofs_getter, line_getter, tester):
  """Get from cache and update cache.

  To create an empty cache, set ab to [].

  The cache contains 0, or 1 or 2 entries. Each entry is a list of format
  [fofs, line, ofs].

  Returns:
    List or tuple of the form [fofs, g, dummy], where g is a bool indicating
    the test result (x-is-less-than-line or x-is-less-or-equal-to-line). The
    dummy value is useless to the caller.
  """
  # TODO(pts): Add support for caches larger than 2 (possibly unlimited).
  assert len(ab) <= 2
  if ab and ab[0][2] <= ofs <= ab[0][0]:
    ab.reverse()  # Move ab[0] to the end since we've just fetched it.
  elif len(ab) > 1 and ab[-1][2] <= ofs <= ab[-1][0]:
    pass
  else:
    fofs = fofs_getter(ofs, size)
    assert 0 <= ofs <= fofs
    if ab and ab[0][0] == fofs:
      ab.reverse()  # Move ab[0] to the end since we've just fetched it.
      if ab[-1][2] > ofs:
        ab[-1][2] = ofs
    elif len(ab) > 1 and ab[-1][0] == fofs:
      if ab[-1][2] > ofs:
        ab[-1][2] = ofs
    else:
      if len(ab) > 1:  # Don't keep more than 2 items in the cache.
        del ab[0]
      # In C we can optimize the combination of line_getter and tester: we
      # no need to keep the line in mem, we can compare on-the-fly.
      ab.append([fofs, tester(line_getter(fofs, size)), ofs])
  return ab[-1]  # Return the most recent item of the cache.


class LineBisecter(object):
  """Bisection (binary) search on newline-separated, sorted file lines.

  If you use sort(1) to sort the file, run it as `LC_ALL=C sort' to make it
  lexicographically sorted, ignoring locale.

  Methods are not thread-safe! They use the file object and the cache (if any)
  as a shared resource.
  """

  __slots__ = ('f',)

  def __init__(self, f):
    self.f = f

  def _readline_at_fofs(self, fofs, size):
    """Returns line at fofs, or () on EOF (or truncation at size)."""
    if fofs < 0:
      raise ValueError('Negative line read offset.')
    f = self.f
    if fofs >= size:
      return ()
    # TODO(pts): Maintain an offset map and a line cache.
    if f.tell() != fofs:  # `if' needed to prevent unnecessary lseek(2) call.
      f.seek(fofs)
    line = f.readline()
    if not line:
      return ()
    line = line.rstrip('\n')
    if fofs + len(line) > size:
      line = line[:size - fofs]
    return line

  def _get_fofs(self, ofs, size):
    """Returns fofs (fofs >= ofs)."""
    if not ofs:
      return 0
    if ofs < 0:
      raise ValueError('Negative line read offset.')
    if ofs >= size:
      return size
    f = self.f
    ofs -= 1
    f.seek(ofs)
    line = f.readline()
    if line:
      return min(size, ofs + len(line))
    return ofs

  # We encode EOF as () and we use the fact that it is larger than any string.
  assert '' < ()
  assert '\xff' < ()
  assert '\xff' * 5 < ()

  def bisect_way(self, x, is_left, size=None):
    """Return the smallest offset where to insert line x.

    With is_left being true, emulates bisect_left, otherwise emulates
    bisect_right.

    TODO(pts): Why aren't we passing lo and hi?
    """
    x = x.rstrip('\n')
    if is_left and not x:  # Shortcut.
      return 0
    if size is None:
      self.f.seek(0, 2)
      size = self.f.tell()
    cache = []
    fofs_getter = self._get_fofs
    line_getter = self._readline_at_fofs
    if is_left:
      tester = x.__le__  # x <= y.
    else:
      tester = x.__lt__  # x < y.
    lo, hi, mid = 0, size, 1
    while lo < hi:
      mid = (lo + hi) >> 1
      midf, g, _ = _get_using_cache(
          cache, mid, size, fofs_getter, line_getter, tester)
      if g:
        hi = mid
      else:
        lo = mid + 1
    if mid != lo:
      midf = _get_using_cache(
          cache, lo, size, fofs_getter, line_getter, tester)[0]
    return midf

  def bisect_right(self, x, size=None):
    """Return the largest offset where to insert line x.

    Similar to bisect.bisect_right.

    The return value i is such that all e in a[:i] have e <= x, and all e in
    a[i:] have e > x.  So if x already appears in the list, a.insert(x) will
    insert just after the rightmost x already there.

    If size is not None, then everything after the first size bytes of the file
    are ignored.
    """
    # TODO(pts): Test this.
    return self.bisect_way(x, False, size)

  def bisect_left(self, x, size=None):
    """Return the smallest offset where to insert line x.

    Similar to bisect.bisect_left.

    The return value i is such that all e in a[:i] have e < x, and all e in
    a[i:] have e >= x.  So if x already appears in the list, a.insert(x) will
    insert just before the leftmost x already there.

    Optional args lo (default 0) and hi (default len(a)) bound the
    slice of a to be searched.

    If size is not None, then everything after the first size bytes of the file
    are ignored.
    """
    return self.bisect_way(x, True, size)

  def bisect_interval(self, x, y=None, is_open=False, size=None):
    """Returns (start, end) offset pairs for lines between x and y.

    If is_open is true, then the interval consits of lines x <= line < y.
    Otherwise the interval consists of lines x <= line <= y.
    """
    x = x.rstrip('\n')
    if y is None:
      y = x
    else:
      y = y.strip('\n')
    end = self.bisect_way(y, is_open, size)
    if is_open and x == y:
      return end, end
    else:
      return self.bisect_left(x, end), end


def test_extra(extra_len):
  import cStringIO
  a = cStringIO.StringIO(
      '10ten\n20twenty\n30\n30\n30\n30\n30\n40forty' + 'z' * extra_len)
  size = len(a.getvalue()) - extra_len
  lb = LineBisecter(a)
  def bisect_interval(x, y=None, is_open=False):
    start, end = lb.bisect_interval(x, y, is_open, size)
    data = a.getvalue()[:size]
    assert 0 <= start <= end <= size, (start, end, size)
    if start == end:
      return '-' + data[start : start + 5]
    return data[start : end]
  assert bisect_interval('30') == '30\n30\n30\n30\n30\n'
  assert bisect_interval('30', is_open=True) == '-30\n30'
  assert bisect_interval('31') == '-40for'
  assert bisect_interval('31', is_open=True) == '-40for'
  assert bisect_interval('4') == '-40for'
  assert bisect_interval('4', is_open=True) == '-40for'
  assert bisect_interval('40') == '-40for'
  assert bisect_interval('40', is_open=True) == '-40for'
  assert bisect_interval('41') == '-'
  assert bisect_interval('41', is_open=True) == '-'
  assert bisect_interval('25') == '-30\n30'
  assert bisect_interval('25', is_open=True) == '-30\n30'
  assert bisect_interval('15') == '-20twe'
  assert bisect_interval('15', is_open=True) == '-20twe'
  assert bisect_interval('1') == '-10ten'
  assert bisect_interval('1', is_open=True) == '-10ten'
  assert bisect_interval('') == '-10ten'
  assert bisect_interval('', is_open=True) == '-10ten'
  assert bisect_interval('10ten') == '10ten\n'
  assert bisect_interval('10ten', is_open=True) == '-10ten'
  assert bisect_interval('10ten\n\n\n') == '10ten\n'
  assert bisect_interval('10', '20') == '10ten\n'
  assert bisect_interval('10', '20', True) == '10ten\n'
  assert bisect_interval('10', '20twenty') == '10ten\n20twenty\n'
  assert bisect_interval('10', '20twenty', is_open=True) == '10ten\n'
  assert bisect_interval('10', '30') == '10ten\n20twenty\n30\n30\n30\n30\n30\n'
  assert bisect_interval('10', '30', True) == '10ten\n20twenty\n'


def test():
  test_extra(0)
  test_extra(1)
  test_extra(2)
  test_extra(42)
  print 'pts_line_bisect OK.'


if __name__ == '__main__':
  test()
