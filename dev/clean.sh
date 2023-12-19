#!/bin/bash
docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)
mkdir -p ${HOME}/minio/data2