# Build the manager binary
FROM golang:1.16 as builder

# Package used to track changes to the source code and autocompile
RUN go get github.com/githubnemo/CompileDaemon

ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /workspace

# install curl
RUN apt-get update \
    && apt-get install -y curl

RUN curl -LJs https://github.com/kubernetes-sigs/kubebuilder/releases/download/v2.3.1/kubebuilder_2.3.1_linux_amd64.tar.gz | tar xz
ENV KUBEBUILDER_ASSETS=/workspace/kubebuilder_2.3.1_linux_amd64/bin

# install redis cli
RUN cd /tmp &&\
    curl http://download.redis.io/redis-stable.tar.gz | tar xz &&\
    make -C redis-stable &&\
    cp redis-stable/src/redis-cli /bin &&\
    rm -rf /tmp/redis-stable

# The source code will be mounted from the local storage to /app (via Telepresence)
# This allows CompileDaemon to track the files we are changing in real time
WORKDIR /app

ARG NAMESPACE_ARG="default"
ARG METRICS_ADDR_ARG="0.0.0.0:9808"
ARG ENABLE_LEADER_ELECTION_ARG="true"
ARG DEVMODE_ARG="true"
ARG REDIS_USERNAME_ARG="admin"
ARG REDISAUTH_CLI_ARG="adminpass"
ARG CLUSTER_VIEW_FILE_ARG="cluster_data"
ARG CLUSTER_STATE_FILE_ARG="cluster_state"

ENV NAMESPACE_ENV=${NAMESPACE_ARG}
ENV METRICS_ADDR_ENV=${METRICS_ADDR_ARG}
ENV ENABLE_LEADER_ELECTION_ENV=${ENABLE_LEADER_ELECTION_ARG}
ENV DEVMODE_ENV=${DEVMODE_ARG}
ENV REDIS_USERNAME=${REDIS_USERNAME_ARG}
ENV REDISAUTH_CLI=${REDISAUTH_CLI_ARG}
ENV CLUSTER_VIEW_FILE=${CLUSTER_STATCLUSTER_VIEW_FILE_ARGE_FILE_ARG}
ENV CLUSTER_STATE_FILE=${CLUSTER_STATE_FILE_ARG}

ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on

ENTRYPOINT CompileDaemon --build="go build -o bin/manager main.go" --command="./bin/manager -namespace=$NAMESPACE_ENV -metrics-addr=$METRICS_ADDR_ENV -enable-leader-election=$ENABLE_LEADER_ELECTION_ENV -devmode=$DEVMODE_ENV"

