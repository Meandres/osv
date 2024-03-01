#!/bin/bash

UDOS_GIT=../udos
VMCACHE_GIT=../vmcache

if [ ! -d "$UDOS_GIT" -a -d "$VMCACHE_GIT" ]; then
    echo "Please setup the environement correctly"
    exit 1;
fi

./scripts/build -j $(nproc) image="DBmockup"
