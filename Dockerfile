# Build the manager binary
FROM golang:1.16 as builder

ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /workspace

# install curl
RUN apt-get update \
    && apt-get install -y curl \ 
    && apt-get install unzip

# install redis cli
# RUN cd /tmp &&\
    # curl http://download.redis.io/redis-stable.tar.gz | tar xz &&\
    # make -C redis-stable &&\
    # cp redis-stable/src/redis-cli /bin &&\
    # rm -rf /tmp/redis-stable

RUN cd /tmp &&\
    curl -LO https://github.com/NataliAharoniPayu/redis/archive/refs/heads/unstable.zip
RUN cd /tmp && unzip unstable.zip && \
    make -C redis-unstable &&\
    cp redis-unstable/src/redis-cli /bin &&\
    rm -rf /tmp/redis-unstable

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
COPY server/ server/
COPY data/ data/

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -o manager main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/base-debian11
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=builder /bin/redis-cli .
COPY --from=builder /bin ./bin
COPY --from=builder /lib ./lib
USER nonroot:nonroot
ENV PATH="./:${PATH}"

ENTRYPOINT ["/manager"]