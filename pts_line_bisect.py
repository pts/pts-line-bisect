#! /usr/bin/python
# by pts@fazekas.hu at Fri Nov 29 19:20:15 CET 2013

"""Newline-separated file line bisection algorithms."""


def _get_using_cache(ab, ofs, f, size, tester):
  """Get from cache and update cache.

  To create an empty cache, set ab to [].

  The cache contains 0, or 1 or 2 entries. Each entry is a list of format
  [fofs, line, ofs].

  Returns:
    List or tuple of the form [fofs, g, dummy], where g is a bool indicating
    the test result (x-is-less-than-line or x-is-less-or-equal-to-line). The
    dummy value is useless to the caller.
  """
  assert len(ab) <= 2
  assert ofs < size
  if ab and ab[0][2] <= ofs <= ab[0][0]:
    ab.reverse()  # Move ab[0] to the end since we've just fetched it.
  elif len(ab) > 1 and ab[-1][2] <= ofs <= ab[-1][0]:
    pass
  else:
    if ofs:
      if f.tell() != ofs - 1:  # Avoid lseek(2) call if not needed.
        f.seek(ofs - 1)
      f.readline()
      # Calling f.tell() is cheap, because Python caches the lseek(2) retval.
      fofs = min(size, f.tell())
    else:
      fofs = 0
    assert 0 <= ofs <= fofs <= size
    if ab and ab[0][0] == fofs:
      ab.reverse()  # Move ab[0] to the end since we've just fetched it.
      if ab[-1][2] > ofs:
        ab[-1][2] = ofs
    elif len(ab) > 1 and ab[-1][0] == fofs:
      if ab[-1][2] > ofs:
        ab[-1][2] = ofs
    else:
      g = True  # EOF is always larger than any line we search for.
      if fofs < size:
        if not fofs and f.tell():
          f.seek(0)
        line = f.readline()  # We read at f.tell() == fofs.
        if f.tell() > size:
          line = line[:size - fofs]
        if line:
          g = tester(line.rstrip('\n'))
      if len(ab) > 1:  # Don't keep more than 2 items in the cache.
        del ab[0]
      ab.append([fofs, g, ofs])
  return ab[-1]  # Return the most recent item of the cache.


def bisect_way(f, x, is_left, size=None):
  """Return the smallest offset where to insert line x.

  Bisection (binary) search on newline-separated, sorted file lines.
  If you use sort(1) to sort the file, run it as `LC_ALL=C sort' to make it
  lexicographically sorted, ignoring locale.

  With is_left being true, emulates bisect_left, otherwise emulates
  bisect_right.

  Methods are not thread-safe! They use the file object and the cache (if any)
  as a shared resource.

  TODO(pts): Document why we are not passing lo and hi (ofs vs fofs).
  """
  x = x.rstrip('\n')
  if is_left and not x:  # Shortcut.
    return 0
  if size is None:
    f.seek(0, 2)
    size = f.tell()
  if size <= 0:  # Shortcut.
    return 0
  if is_left:
    tester = x.__le__  # x <= y.
  else:
    tester = x.__lt__  # x < y.
  lo, hi, mid, cache = 0, size - 1, 1, []
  while lo < hi:
    mid = (lo + hi) >> 1
    midf, g, _ = _get_using_cache(cache, mid, f, size, tester)
    if g:
      hi = mid
    else:
      lo = mid + 1
  if mid != lo:
    midf = _get_using_cache(cache, lo, f, size, tester)[0]
  return midf


def bisect_right(f, x, size=None):
  """Return the largest offset where to insert line x.

  Similar to bisect.bisect_right.

  The return value i is such that all e in a[:i] have e <= x, and all e in
  a[i:] have e > x.  So if x already appears in the list, a.insert(x) will
  insert just after the rightmost x already there.

  If size is not None, then everything after the first size bytes of the file
  are ignored.
  """
  return bisect_way(f, x, False, size)


def bisect_left(f, x, size=None):
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
  return bisect_way(f, x, True, size)


def bisect_interval(f, x, y=None, is_open=False, size=None):
  """Returns (start, end) offset pairs for lines between x and y.

  If is_open is true, then the interval consits of lines x <= line < y.
  Otherwise the interval consists of lines x <= line <= y.
  """
  x = x.rstrip('\n')
  if y is None:
    y = x
  else:
    y = y.strip('\n')
  end = bisect_way(f, y, is_open, size)
  if is_open and x == y:
    return end, end
  else:
    return bisect_way(f, x, True, end), end


def test_extra(extra_len):
  import cStringIO
  f = cStringIO.StringIO(
      '10ten\n20twenty\n30\n30\n30\n30\n30\n40forty' + 'z' * extra_len)
  size = len(f.getvalue()) - extra_len
  def xbisect_interval(x, y=None, is_open=False):
    start, end = bisect_interval(f, x, y, is_open, size)
    assert 0 <= start <= end <= size, (start, end, size)
    data = f.getvalue()[:size]
    if start == end:
      return '-' + data[start : start + 5]
    return data[start : end]
  assert xbisect_interval('30') == '30\n30\n30\n30\n30\n'
  assert bisect_left(f, '30', size) == 15
  assert bisect_right(f, '30', size) == 30
  assert bisect_left(f, '32', size) == 30
  assert bisect_right(f, '32', size) == 30
  assert xbisect_interval('30', is_open=True) == '-30\n30'
  assert xbisect_interval('31') == '-40for'
  assert xbisect_interval('31', is_open=True) == '-40for'
  assert xbisect_interval('4') == '-40for'
  assert xbisect_interval('4', is_open=True) == '-40for'
  assert xbisect_interval('40') == '-40for'
  assert xbisect_interval('40', is_open=True) == '-40for'
  assert xbisect_interval('41') == '-'
  assert xbisect_interval('41', is_open=True) == '-'
  assert xbisect_interval('25') == '-30\n30'
  assert xbisect_interval('25', is_open=True) == '-30\n30'
  assert xbisect_interval('15') == '-20twe'
  assert xbisect_interval('15', is_open=True) == '-20twe'
  assert xbisect_interval('1') == '-10ten'
  assert xbisect_interval('1', is_open=True) == '-10ten'
  assert xbisect_interval('') == '-10ten'
  assert xbisect_interval('', is_open=True) == '-10ten'
  assert xbisect_interval('10ten') == '10ten\n'
  assert xbisect_interval('10ten', is_open=True) == '-10ten'
  assert xbisect_interval('10ten\n\n\n') == '10ten\n'
  assert xbisect_interval('10', '20') == '10ten\n'
  assert xbisect_interval('10', '20', True) == '10ten\n'
  assert xbisect_interval('10', '20twenty') == '10ten\n20twenty\n'
  assert xbisect_interval('10', '20twenty', is_open=True) == '10ten\n'
  assert xbisect_interval('10', '30') == '10ten\n20twenty\n30\n30\n30\n30\n30\n'
  assert xbisect_interval('10', '30', True) == '10ten\n20twenty\n'


def test():
  test_extra(0)
  test_extra(1)
  test_extra(2)
  test_extra(42)
  print 'pts_line_bisect OK.'


if __name__ == '__main__':
  test()
