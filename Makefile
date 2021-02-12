# TODO need to make a 'clean' target for removing the cluster resources and local files
# Image URL to use all building/pushing image targets
IMG := redis-operator-docker:local
DEV_IMAGE ?= redis-operator:dev
DEPLOY_TARGET ?= deploy-default

TEST := test
ENVCONFIG := default
CONFIG_ENV := config-build-local

OPERATOR_NAMESPACE ?= "default"
METRICS_ADDR ?= "0.0.0.0:9808"
ENABLE_LEADER_ELECTION ?= "true"

ifdef NOTEST
	TEST := skip
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

# Setup e2e tests
e2e-test-setup: IMG=redis-operator-docker:local
e2e-test-setup: docker-build-operator docker-build-local-redis docker-build-local-redis-init docker-build-local-metrics-exporter kind-load-all
	docker build ./hack -f ./hack/redis.Dockerfile -t redis:update
	kind load docker-image redis:update --name $(CLUSTER_NAME)

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

# Builds the resources from kustomize configuration based on the specified environment
config-build: $(CONFIG_ENV)
	cd config/manager/base && kustomize edit set image controller=$(IMG)

# Use kustomize to build the YAML configuration files for the default (local) setup
config-build-local:
	kustomize build config/default | kubectl apply -f -

# Deploy controller in the configured Kubernetes cluster in ~/.kube/config
deploy-default: generate manifests config-build docker-build-operator docker-build-local-redis docker-build-local-redis-init docker-build-local-metrics-exporter docker-build-local-test-client kind-load-all

# Generate manifests e.g. CRD, RBAC etc.
manifests: controller-gen
	$(CONTROLLER_GEN) $(CRD_OPTIONS) rbac:roleName=manager paths="./..." output:crd:artifacts:config=config/crd/bases

# Run go fmt against code
fmt:
	go fmt ./...

# Run go vet against code
vet:
	go vet ./...

# Generate code
generate: controller-gen
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

# Build the operator docker image
docker-build-operator: $(TEST)
	docker build . -t $(IMG) --build-arg NAMESPACE=$(OPERATOR_NAMESPACE) --build-arg METRICS_ADDR=$(METRICS_ADDR) --build-arg ENABLE_LEADER_ELECTION=$(ENABLE_LEADER_ELECTION)

# TODO add the operator flags to the developmetn image too
# Build the development Docker image
docker-build-dev: $(TEST)
	docker build ./hack -f ./hack/dev.Dockerfile -t $(DEV_IMAGE)

# Builds a local image for the Redis pods from the latest Dockerhub image
docker-build-local-redis:
	docker build ./hack -f ./hack/redis.Dockerfile -t redis:testing

docker-build-local-redis-init:
	docker build ./hack -f ./hack/redis-init.Dockerfile -t redis-init:testing

docker-build-local-metrics-exporter:
	docker build ./hack -f ./hack/metrics-exporter.Dockerfile -t metrics-exporter:testing

docker-build-local-test-client:
	docker build ./hack/redis-client-app -f ./hack/redis-client-app/Dockerfile -t redis-client-app:testing

# Push the docker image
docker-push:
	docker push $(IMG)

# Load all required images
kind-load-all: kind-load-redis-init kind-load-metrics-exporter kind-load-controller kind-load-redis kind-load-redis-client-app

# Load the controller image on the nodes of a kind cluster
kind-load-controller:
	kind load docker-image $(IMG) --name $(CLUSTER_NAME)

# Load the local redis image
kind-load-redis:
	kind load docker-image redis:testing --name $(CLUSTER_NAME)

# Load the local init container for redis image
kind-load-redis-init:
	kind load docker-image redis-init:testing --name $(CLUSTER_NAME)

# Load the local metrics exporter container for redis
kind-load-metrics-exporter:
	kind load docker-image metrics-exporter:testing --name $(CLUSTER_NAME)

# Load the local metrics exporter container for redis
kind-load-redis-client-app:
	kind load docker-image redis-client-app:testing --name $(CLUSTER_NAME)	

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
