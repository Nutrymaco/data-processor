#!/bin/bash
mkdir -p ${HOME}/minio/data
docker run \
   -d \
   -p 9000:9000 \
   -p 9090:9090 \
   --user $(id -u):$(id -g) \
   --name minio1 \
   -e "MINIO_ROOT_USER=USER" \
   -e "MINIO_ROOT_PASSWORD=PASSWORD" \
   -v ${HOME}/minio/data:/data \
   quay.io/minio/minio server /data  --address ":9000" --console-address ":9090"

mkdir -p ${HOME}/minio/data2
docker run \
   -d \
   -p 8000:8000 \
   -p 8080:8080 \
   --user $(id -u):$(id -g) \
   --name minio2 \
   -e "MINIO_ROOT_USER=USER" \
   -e "MINIO_ROOT_PASSWORD=PASSWORD" \
   -v ${HOME}/minio/data2:/data \
   quay.io/minio/minio server /data --address ":8000" --console-address ":8080"