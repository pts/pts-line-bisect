README for pts_lbsearch
^^^^^^^^^^^^^^^^^^^^^^^
pts_lbsearch is a fast binary search in a line-sorted text file. It finds
and prints lines within a given range or with a given prefix. It works
even if lines have variable length. pts_lbsearch is written in C, it is
optimized for speed and it is using buffering and cache to avoid unnecessary
read(2)s and lseek(2)s. It also has a very small, constant memory footprint:
barely larger than the read buffer (8KB by default).

pts_lbsearch gives correct results only if the bytes of the input file are
sorted lexicographically. Use LC_ALL=C to get 8-bit, bytewise,
locale-independent sort(1):

  $ LC_ALL=C sort <file >file.sorted

Prefix search: print all lines starting with foo, sorted:

  $ pts_lbsearch -p file.sorted foo

Prefix search: print all lines starting with foo, sorted, but ignore the
last incomplete line (if any):

  $ pts_lbsearch -p file.sorted foo

Prefix range search: print all lines starting with bar or foo or between:

  $ pts_lbsearch -p file.sorted bar foo

Closed ended interval search: print all lines at least foo and at most bar:

  $ pts_lbsearch -t file.sorted bar foo

Open ended interval search: print all lines at least foo and smaller than bar:

  $ pts_lbsearch -e file.sorted bar foo

Prefix detection: exit(0) iff a line starting with foo is present:

  $ pts_lbsearch -qp file.sorted foo

Exact detection: exit(0) iff a line equals to foo is present:

  $ pts_lbsearch -qt file.sorted foo

Insert position: print the smallest offset where foo can be inserted (please
note that this does not report whether foo is present, always exit(0)).

  $ pts_lbsearch -oe file.sorted foo

Prefix range: print the range (start byte offset and after-the-last-byte end
offset) of lines starting with foo:

  $ pts_lbsearch -op file.sorted foo

Exact range: print the range (start byte offset and after-the-last-byte end
offset) of lines containing only foo:

  $ pts_lbsearch -ot file.sorted foo

pts_lbsearch supports only '\n' as the line separator. '\r' is treated as a
regular character within the line.

pts_lbsearch supports incomplete lines at the end of the input. When
printing them, it never adds a '\n' to the end if there wasn't one. To make
it ignore the incomplete last line (possibly because an other, slow process
has not finished writing it), pass the `-i' flag.

If the input is not sorted, pts_lbsearch will most probably less than
expected. Otherwise it would work correctly (e.g. no segmentation fault).

pts_lbsearch works for large files (i.e. larger than 2GB) correctly.

Please note that a lookup in a btree or hash index is usually faster than a
binary search, beause btree and hash need much fewer disk seeks because of
the large branching factor. So if you can afford to build an index, there
are faster lookup solutions than binary search. But if indeed you want
binary search on variable-length records, most probably pts_lbsearch is the
fastest.

Developer documentation
~~~~~~~~~~~~~~~~~~~~~~~
Differences in the C and Python implementations:

* The Python implementation is just a library, it has no command-line
  interface.
* The Python implementation has tests.
* The C implementation supports prefix search (CM_LP).
* The C implementation avoids lseek(2) and read(2) calls as much as
  possible, while the Python implementation doesn't, because in Python the
  `file' object discards the read buffer after each file.seek.
* The C equivalent of bisect_interval does a CM_LE search and then a CM_LT
  for the rest. This is faster if the result interval is a short range near
  the end of the file.

__EOF__
