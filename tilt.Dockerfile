FROM golang:1.17
WORKDIR /
ADD hack/redis-bin/build/redis-cli /bin/redis-cli
ADD bin/manager /manager
ENTRYPOINT manager
