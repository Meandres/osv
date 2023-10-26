#!/bin/bash 

docker build -t udb -f Dockerfile.udb .

docker run -it -v /home/meandres/01_work/UDB/osv:/git-repos/osv --privileged udb
