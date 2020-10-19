# Image URL to use all building/pushing image targets
IMG ?= docker-registry.zooz.co:4567/payu-clan-sre/redis/redis-operator/redis-operator-docker
DEV_IMAGE ?= redis-operator:dev
DEPLOY_TARGET ?= deploy-default

TEST := test
ENVCONFIG := default
CONFIG_ENV := config-build-local

# Used to specify what environment is targeted, it determines what kustomize config is built
# by default it builds the config for the local kind cluster.
# To build for development environment run with envconfig=dev.
ifeq ($(envconfig), dev)
	CONFIG_ENV = config-build-development
endif

ifdef NOTEST
	TEST := skip
endif

ifdef LOCAL
	IMG := redis-operator-docker:local
	REDIS_LOAD := kind-load-redis
	REDIS_BUILD := docker-build-local-redis
	DEPLOY_TARGET := deploy-local
endif

CLUSTER_NAME ?= redis-test
# Produce CRDs that work back to Kubernetes 1.11 (no version conversion)
CRD_OPTIONS ?= "crd:trivialVersions=true"

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

all: manager

# Run tests
test: generate fmt vet manifests
	go test ./... -coverprofile cover.out

# Run e2e tests
e2e-test: IMG=redis-operator-docker:local
e2e-test: docker-build docker-build-local-redis kind-load-redis kind-load-controller
	cd test/e2e && go test -count=1 ./... -tags=e2e_redis_op

# Build manager binary
manager: generate fmt vet
	go build -o bin/manager main.go

# Run against the configured Kubernetes cluster in ~/.kube/config
run: generate fmt vet manifests
	go run ./main.go

# Install CRDs into a cluster
install: manifests
	kustomize build config/crd | kubectl apply -f -

# Uninstall CRDs from a cluster
uninstall: manifests
	kustomize build config/crd | kubectl delete -f -

deploy: $(DEPLOY_TARGET)

# Deploy controller in the configured Kubernetes cluster in ~/.kube/config
deploy-default: manifests config-build

# Builds the resources from kustomize configuration based on the specified environment
config-build: $(CONFIG_ENV)
	cd config/manager/base && kustomize edit set image controller=$(IMG)

# Use kustomize to build the YAML configuration files for the default (local) setup
config-build-local:
	kustomize build config/default | kubectl apply -f -

# Use kustomize to build the YAML configuration files for the development cluster
config-build-development:
	kustomize build config/development | kubectl apply -f -

# Deploy controller in a local kind cluster
deploy-local: manifests docker-build $(REDIS_BUILD) $(REDIS_LOAD) kind-load-controller deploy-default

# Generate manifests e.g. CRD, RBAC etc.
manifests: controller-gen
	$(CONTROLLER_GEN) $(CRD_OPTIONS) rbac:roleName=manager-role webhook paths="./..." output:crd:artifacts:config=config/crd/bases

# Run go fmt against code
fmt:
	go fmt ./...

# Run go vet against code
vet:
	go vet ./...

# Generate code
generate: controller-gen
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

# Build the docker image
docker-build: $(TEST)
	docker build . -t $(IMG)

# Build the development Docker image
docker-build-dev: $(TEST)
	docker build ./hack -f ./hack/dev.Dockerfile -t $(DEV_IMAGE)

# Builds a local image for the Redis pods from the latest Dockerhub image
docker-build-local-redis:
	docker build ./hack -f ./hack/redis.Dockerfile -t redis:testing

# Push the docker image
docker-push:
	docker push $(IMG)

# Load the controller image on the nodes of a kind cluster
kind-load-controller:
	kind load docker-image $(IMG) --name $(CLUSTER_NAME)

# Load the local redis image
kind-load-redis:
	kind load docker-image redis:testing --name $(CLUSTER_NAME)

# Used for skipping targets
skip: ;

# find or download controller-gen
# download controller-gen if necessary
controller-gen:
ifeq (, $(shell which controller-gen))
	@{ \
	set -e ;\
	CONTROLLER_GEN_TMP_DIR=$$(mktemp -d) ;\
	cd $$CONTROLLER_GEN_TMP_DIR ;\
	go mod init tmp ;\
	go get sigs.k8s.io/controller-tools/cmd/controller-gen@v0.2.5 ;\
	rm -rf $$CONTROLLER_GEN_TMP_DIR ;\
	}
CONTROLLER_GEN=$(GOBIN)/controller-gen
else
CONTROLLER_GEN=$(shell which controller-gen)
endif
