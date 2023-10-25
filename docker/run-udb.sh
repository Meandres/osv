#! /usr/bin/env nix-shell

docker build -t osv/udb -f Dockerfile.udb .
docker run -it --privileged osv/udb
