#!/bin/bash

# 2023 Lars Quentin

# Call it with
# ./scriptname ./path/to/your/Dockerfile
#
if [ "$EUID" -ne 0 ]
  then echo "Please run as root"
  exit
fi

if [ $# -ne 1 ]
  then echo "USAGE: ./scriptname ./path/to/your/Dockerfile"
  exit
fi


docker_filename="./build/docker_.tar.gz"
singularity_filename="./build/singularity_.sif"

sudo rm -rf ./build
mkdir build

docker save "$(sudo docker build -q -f $1 .)" -o $docker_filename && \
singularity build $singularity_filename docker-archive://$docker_filename
