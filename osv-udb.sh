#!/bin/bash

./scripts/setup.py
./scripts/build -j $(nproc) conf_drivers_nvme=1 image="vmcache"

# to run normally (print bt aswell so you can run this to have an idea of the problem even if gdb's bt is more precise)
# ./scripts/run.py --nvme
# to run detached
# ./scripts/run.py -d --nvme -D 
# then
# gdb build/debug/loader.elf
# inside gdb : (note : this works only when OSv fails with abort() which hangs the VM letting us connect to it)
# 	- osv syms
# 	- connect
# since we opened qemu in the background, we can kill it with
# kill -9 $(ps -aux | grep qemu-system | grep -v grep | awk '{ print $2; }')
#
# To connect with another terminal :
# docker ps -> CONTAINER_ID
# docker exec -it CONTAINER_ID bash

