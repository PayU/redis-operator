# Build the manager binary
FROM golang:1.15 as builder

ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /workspace

# install curl
RUN apt-get update \
    && apt-get install -y curl

# install redis cli
RUN cd /tmp &&\
    curl http://download.redis.io/redis-stable.tar.gz | tar xz &&\
    make -C redis-stable &&\
    cp redis-stable/src/redis-cli /bin &&\
    rm -rf /tmp/redis-stable

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY main.go main.go
COPY api/ api/
COPY controllers/ controllers/

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -o manager main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/base-debian10
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=builder /bin/redis-cli .
COPY --from=builder /bin/sh .
USER nonroot:nonroot
ENV PATH="./:${PATH}"

ARG NAMESPACE_ARG="default"
ARG METRICS_ADDR_ARG="0.0.0.0:9808"
ARG ENABLE_LEADER_ELECTION_ARG="true"

ENV NAMESPACE_ENV=${NAMESPACE_ARG}
ENV METRICS_ADDR_ENV=${METRICS_ADDR_ARG}
ENV ENABLE_LEADER_ELECTION_ENV=${ENABLE_LEADER_ELECTION_ARG}

ENTRYPOINT ["/sh", "-c", "/manager -namespace=$NAMESPACE_ENV -metrics-addr=$METRICS_ADDR_ENV -enable-leader-election=$ENABLE_LEADER_ELECTION_ENV"]
