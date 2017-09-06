#! /bin/sh
set -ex
i686-w64-mingw32-gcc -s -O2 \
    -W -Wall -Wextra \
    -Werror=missing-declarations -Werror=implicit-function-declaration \
    -ansi -o pts_lbsearch.exe ./pts_lbsearch.c
ls -l pts_lbsearch.exe
: compile_mingw.sh OK.
