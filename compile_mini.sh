#! /bin/sh
# minicc is part of https://github.com/pts/minilibc686
set -ex
# Uses --wcc (OpenWatcom C compiler) by default, that gives shorter output
# for this program than GCC.
minicc -o pts_lbsearch.mini -fomit-frame-pointer pts_lbsearch.c
ls -l pts_lbsearch.mini
: compile_xtiny.sh OK.
