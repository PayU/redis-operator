#!/bin/bash

if [[ -d "build" ]]
then
  echo "found redis build dir"
  exit 0
fi

echo "Building redis..."
docker build . -t redis-builder:dev
docker run --rm -v $(pwd)/build:/redis redis-builder:dev
