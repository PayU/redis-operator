FROM golang:1.15
WORKDIR /
ADD hack/redis-bin/build/redis-cli /bin/redis-cli
ADD bin/manager /manager
ENTRYPOINT manager
