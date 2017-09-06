#! /bin/sh
set -ex
xstatic gcc -s -O2 \
    -W -Wall -Wextra \
    -Werror=missing-declarations -Werror=implicit-function-declaration \
    -ansi -o pts_lbsearch.xstatic ./pts_lbsearch.c
ls -l pts_lbsearch.xstatic
: compile_xstatic.sh OK.
