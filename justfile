proot := justfile_directory()
id_nvme := "c3:00.0"

default:
    @just --choose

help:
    just --list

build image="":
    #!/usr/bin/env bash
    ./scripts/build -j image={{image}}

run nb_cpu="1" mem_size="50G" cmd="":
    #!/usr/bin/env bash
    ./scripts/run.py -c {{nb_cpu}} -m {{mem_size}} --nvme {{id_nvme}} --mount-fs=virtiofs,/dev/virtiofs0,/virtiofs --virtio-fs-tag=myfs --virtio-fs-dir=shared_dir -e \"{{cmd}}\"
