# should build the redis binary with the same image that the operator will run
FROM golang:1.16

ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /
RUN apt-get update && apt-get install -y curl
COPY build-redis.sh /build-redis.sh
RUN mkdir /redis

ENTRYPOINT ["./build-redis.sh"]
