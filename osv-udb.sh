#!/bin/bash

# To launch docker container (dirty and dev env) :
# go in docker/
# docker build -t udb -f Dockerfile.udb .
# docker run -it -v /home/meandres/01_work/UDB/osv:/git-repos/osv --privileged udb
# replace the path with the location of osv on your system 

apt update && apt upgrade -y
./scripts/setup.py
dd if=/dev/zero of=nvme.img bs=1M count=4096
./scripts/build conf_drivers_nvme=1 image="nvme-test"

# to run detached
# ./scripts/run.py -d --nvme -D 
# then
# gdb build/debug/loader.elf
# inside gdb : (note : this works only when OSv fails with abort() which hangs the VM letting us connect to it)
# 	- osv syms
# 	- connect
# since we opened qemu in the background, we can kill it with
# kill -9 $(ps -aux | grep qemu-system | grep -v grep | awk '{ print $2; }')
