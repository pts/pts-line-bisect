#! /usr/bin/python
# by pts@fazekas.hu at Fri Nov 29 19:20:15 CET 2013

"""Newline-separated file line bisection algorithms."""

# TODO(pts): Remove once development has finished.
_hitc = 0
_xhitc = 0
_allc = 0


def _get_using_cache(ab, ofs, fofs_getter, line_getter):
  """Get from cache and update cache.

  To create an empty cache, set ab to [].

  The cache contains 0, or 1 or 2 entries. Each entry is a list of format
  [fofs, line, ofs].

  Returns:
    List or tuple of the form [fofs, line, dummy]. The dummy value is useless
    to the caller.
  """
  # TODO(pts): Add support for caches larger than 2 (possibly unlimited).
  global _hitc, _xhitc, _allc
  _allc += 1
  assert len(ab) <= 2
  if ab and ab[0][2] <= ofs <= ab[0][0]:
    _xhitc += 1
    ab.reverse()  # Move ab[0] to the end since we've just fetched it.
  elif len(ab) > 1 and ab[-1][2] <= ofs <= ab[-1][0]:
    _xhitc += 1
  else:
    fofs = fofs_getter(ofs)
    assert 0 <= ofs <= fofs
    if ab and ab[0][0] == fofs:
      _hitc += 1
      ab.reverse()  # Move ab[0] to the end since we've just fetched it.
      if ab[-1][2] > ofs:
        ab[-1][2] = ofs
    elif len(ab) > 1 and ab[-1][0] == fofs:
      _hitc += 1
      if ab[-1][2] > ofs:
        ab[-1][2] = ofs
    else:
      if len(ab) > 1:  # Don't keep more than 2 items in the cache.
        del ab[0]
      # TODO(pts): Don't remember the line, just if it was smaller than x.
      # That way we won't share the cache between lookups of different x.
      # This would be a good C optimization: no need to keep the line in mem.
      ab.append([fofs, line_getter(fofs), ofs])
  return ab[-1]  # Return the most recent item of the cache.


def _get_fofs_using_cache(ab, ofs, fofs_getter):
  """Similar to _get_using_cache, but faster because returns fofs only.

  Doesn't update the cache.

  Returns:
    fofs.
  """
  global _hitc, _xhitc, _allc
  _allc += 1
  assert len(ab) <= 2
  if ab and ab[0][2] <= ofs <= ab[0][0]:
    _xhitc += 1
    ab.reverse()  # Move ab[0] to the end since we've just fetched it.
    return ab[-1][0]
  elif len(ab) > 1 and ab[-1][2] <= ofs <= ab[-1][0]:
    _xhitc += 1
    return ab[-1][0]
  else:
    fofs = fofs_getter(ofs)
    assert 0 <= ofs <= fofs
    if ab and ab[0][0] == fofs:
      _hitc += 1
      ab.reverse()  # Move ab[0] to the end since we've just fetched it.
      if ab[-1][2] > ofs:
        ab[-1][2] = ofs
    elif len(ab) > 1 and ab[-1][0] == fofs:
      _hitc += 1
      if ab[-1][2] > ofs:
        ab[-1][2] = ofs
    else:
      # We don't update the cache, because we don't call line_getter(fofs).
      return fofs


class LineBisecter(object):
  """Bisection (binary) search on newline-separated, sorted file lines.

  If you use sort(1) to sort the file, run it as `LC_ALL=C sort' to make it
  lexicographically sorted, ignoring locale.

  Methods are not thread-safe! They use the file object and the cache (if any)
  as a shared resource.
  """

  __slots__ = ('f', 'size')

  def __init__(self, f, size=None):
    if size is None:
      f.seek(0, 2)
      size = f.tell()
    self.f = f
    self.size = size

  def _readline_at_fofs(self, fofs):
    """Returns line at fofs, or () on EOF (or truncation at self.size)."""
    if fofs < 0:
      raise ValueError('Negative line read offset.')
    size = self.size
    f = self.f
    if fofs >= size:
      return ()
    # TODO(pts): Maintain an offset map and a line cache.
    if f.tell() != fofs:  # `if' needed to prevent unnecessary lseek(2) call.
      f.seek(fofs)
    line = f.readline()
    if not line:
      if self.size > ofs:
        self.size = ofs
      return ()
    line = line.rstrip('\n')
    if fofs + len(line) > size:
      line = line[:size - fofs]
    return line

  def _get_fofs(self, ofs):
    """Returns fofs (fofs >= ofs)."""
    if not ofs:
      return 0
    if ofs < 0:
      raise ValueError('Negative line read offset.')
    if ofs >= self.size:
      return self.size
    f = self.f
    ofs -= 1
    f.seek(ofs)
    line = f.readline()
    if line:
      return min(self.size, ofs + len(line))
    if self.size > ofs:
      self.size = ofs
    return ofs

  # We encode EOF as () and we use the fact that it is larger than any string.
  assert '' < ()
  assert '\xff' < ()
  assert '\xff' * 5 < ()

  def bisect_right(self, x, lo=0, hi=None, cache=None):
    """Return the largest offset where to insert line x.

    To initialize a shared cache, set it to [], and pass it as cache=.

    Similar to bisect.bisect_right.

    The return value i is such that all e in a[:i] have e <= x, and all e in
    a[i:] have e > x.  So if x already appears in the list, a.insert(x) will
    insert just after the rightmost x already there.

    Optional args lo (default 0) and hi (default len(a)) bound the
    slice of a to be searched.
    """
    x = x.rstrip('\n')
    if lo < 0:
      raise ValueError('lo must be non-negative')
    if hi is None or hi > self.size:
      hi = self.size
    if cache is None:
      cache = []
    fofs_getter = self._get_fofs
    if lo >= hi:
      return _get_fofs_using_cache(cache, lo, fofs_getter)
    line_getter = self._readline_at_fofs
    yold = None
    while lo < hi:
      mid = (lo + hi) >> 1
      midf, y, _ = _get_using_cache(cache, mid, fofs_getter, line_getter)
      if y is yold:  # Equivalent test for C: `mid == midold'.
        if gold:  # Avoid possible expensive comparison of x and y.
          hi = mid
        else:
          lo = mid + 1
      else:
        yold = y
        gold = x < y
        if x < y:
          hi = mid
        else:
          lo = mid + 1
    if mid != lo:
      midf = _get_fofs_using_cache(cache, lo, fofs_getter)
    return midf

  def bisect_left(self, x, lo=0, hi=None, cache=None):
    """Return the smallest offset where to insert line x.

    Similar to bisect.bisect_left.

    The return value i is such that all e in a[:i] have e < x, and all e in
    a[i:] have e >= x.  So if x already appears in the list, a.insert(x) will
    insert just before the leftmost x already there.

    Optional args lo (default 0) and hi (default len(a)) bound the
    slice of a to be searched.
    """
    x = x.rstrip('\n')
    if lo < 0:
      raise ValueError('lo must be non-negative')
    if hi is None or hi > self.size:
      hi = self.size
    if not x:  # Shortcut.
      return 0
    if cache is None:
      cache = []
    fofs_getter = self._get_fofs
    if lo >= hi:
      return _get_fofs_using_cache(cache, lo, fofs_getter)
    line_getter = self._readline_at_fofs
    yold = None
    while lo < hi:
      mid = (lo + hi) >> 1
      midf, y, _ = _get_using_cache(cache, mid, fofs_getter, line_getter)
      if y is yold:  # Equivalent test for C: `mid == midold'.
        if gold:  # Avoid possible expensive comparison of x and y.
          hi = mid
        else:
          lo = mid + 1
      else:
        yold = y
        gold = x <= y
        if gold:
          hi = mid
        else:
          lo = mid + 1
    if mid != lo:
      midf = _get_fofs_using_cache(cache, lo, fofs_getter)
    return midf

  def bisect_interval(self, x, y=None, is_closed=True, lo=0, hi=None,
                      cache=None):
    """Returns (start, end) offset pairs for lines between x and y.

    If is_closed is true, then the interval consits of lines x <= line <= y.
    Otherwise the interval consists of lines x <= line < y.
    """
    x = x.rstrip('\n')
    if y is None:
      y = x
    else:
      y = y.strip('\n')
    if cache is None:
      cache = []
    start = self.bisect_left(x, lo, hi, cache)
    if is_closed:
      end = self.bisect_right(y, start, hi, cache)
    else:
      end = self.bisect_left(y, start, hi, cache)
    return start, end

  def bisect_open(self, x, y=None, lo=0, hi=None):
    """Returns (start, end) offset pairs for x <= line < y."""
    x = x.rstrip('\n')
    if y is None:
      y = x
    else:
      y = y.strip('\n')
    start = self.bisect_left(x, lo, hi)
    end = self.bisect_right(y, start, hi)
    return start, end


def test_extra(extra_len):
  import cStringIO
  a = cStringIO.StringIO(
      '10ten\n20twenty\n30\n30\n30\n30\n30\n40forty' + 'z' * extra_len)
  if extra_len:
    lb = LineBisecter(a, len(a.getvalue()) - extra_len)
  else:
    lb = LineBisecter(a)
  def bisect_interval(x, y=None, is_closed=True):
    start, end = lb.bisect_interval(x, y, is_closed)
    data = a.getvalue()[: lb.size]
    assert 0 <= start <= end <= lb.size, (start, end, lb.size)
    if start == end:
      return '-' + data[start : start + 5 ]
    return data[start : end]
  assert bisect_interval('30') == '30\n30\n30\n30\n30\n'
  assert bisect_interval('30', is_closed=False) == '-30\n30'
  assert bisect_interval('31') == '-40for'
  assert bisect_interval('31', is_closed=False) == '-40for'
  assert bisect_interval('4') == '-40for'
  assert bisect_interval('4', is_closed=False) == '-40for'
  assert bisect_interval('40') == '-40for'
  assert bisect_interval('40', is_closed=False) == '-40for'
  assert bisect_interval('41') == '-'
  assert bisect_interval('41', is_closed=False) == '-'
  assert bisect_interval('25') == '-30\n30'
  assert bisect_interval('25', is_closed=False) == '-30\n30'
  assert bisect_interval('15') == '-20twe'
  assert bisect_interval('15', is_closed=False) == '-20twe'
  assert bisect_interval('1') == '-10ten'
  assert bisect_interval('1', is_closed=False) == '-10ten'
  assert bisect_interval('') == '-10ten'
  assert bisect_interval('', is_closed=False) == '-10ten'
  assert bisect_interval('10ten') == '10ten\n'
  assert bisect_interval('10ten', is_closed=False) == '-10ten'
  assert bisect_interval('10ten\n\n\n') == '10ten\n'
  assert bisect_interval('10', '20') == '10ten\n'
  assert bisect_interval('10', '20', False) == '10ten\n'
  assert bisect_interval('10', '20twenty') == '10ten\n20twenty\n'
  assert bisect_interval('10', '20twenty', is_closed=False) == '10ten\n'
  assert bisect_interval('10', '30') == '10ten\n20twenty\n30\n30\n30\n30\n30\n'
  assert bisect_interval('10', '30', False) == '10ten\n20twenty\n'


def test():
  test_extra(0)
  test_extra(1)
  test_extra(2)
  test_extra(42)
  # TODO(pts): Add tests for '\n\n\n' in the beginning.
  # TODO(pts): Add longer strings where _xhitc is larger.
  print 'cache xhit/all=%d%% hit/all=%d%%' % (
      (_xhitc * 100 + (_allc >> 1)) // _allc,
      (_hitc * 100 + (_allc >> 1)) // _allc)
  print 'pts_line_bisect OK.'


if __name__ == '__main__':
  test()
