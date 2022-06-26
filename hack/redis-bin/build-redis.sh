#!/bin/sh

REDIS_VERSION=6.2.6

cd /redis
curl -LJs https://github.com/redis/redis/archive/refs/tags/$REDIS_VERSION.tar.gz | tar xz

make -C redis-$REDIS_VERSION
cp redis-$REDIS_VERSION/src/redis-cli .
