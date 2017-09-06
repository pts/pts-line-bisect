#! /bin/sh
set -ex
xtiny gcc -W -Wall -Wextra \
    -Werror=missing-declarations -Werror=implicit-function-declaration \
    -ansi -o pts_lbsearch.xtiny ./pts_lbsearch.c
ls -l pts_lbsearch.xtiny
: compile_xtiny.sh OK.
