#!/bin/bash

./scripts/setup.py
dd if=/dev/zero of=/path/to/nvme.img bs=1M count=4096
./scripts/build conf_drivers_nvme=1 module="zfs, zfs-tools"
