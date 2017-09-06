#! /bin/sh
set -ex
docker run -v $PWD:/workdir multiarch/crossbuild \
    /usr/osxcross/bin/o64-clang -mmacosx-version-min=10.5 \
    -lSystem -lcrt1.10.5.o -nostdlib \
    -O2 -W -Wall -Wextra \
    -Werror=missing-declarations -Werror=implicit-function-declaration \
    -ansi -o pts_lbsearch.darwinc64 ./pts_lbsearch.c
docker run -v $PWD:/workdir multiarch/crossbuild \
    /usr/osxcross/bin/i386-apple-darwin14-strip \
    pts_lbsearch.darwinc64
ls -l pts_lbsearch.darwinc64
: compile_darwinc64.sh OK.
