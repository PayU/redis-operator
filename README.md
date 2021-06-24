# PayU Redis Operator

Kubernetes operator that creates and manages Redis HA clustered databases - [Redis docs](https://redislabs.com/redis-enterprise/technology/redis-enterprise-cluster-architecture/).<br>
[ Feature state wiki page ](https://github.com/PayU/redis-operator/wiki/Feature-state)

### Using the operator on a local cluster

The project uses a [kind](https://kind.sigs.k8s.io/docs/user/quick-start/) cluster for development and test purposes.

Requirements:

* `kind`: [v0.10.0](https://github.com/kubernetes-sigs/kind/releases/tag/v0.10.0)
* `controller-gen` > 0.4
* `kustomize` >= 4.0

**1. Setting up a cluster**

```bash
cd hack
sh ./install.sh # you might need to run this as sudo if a regular user can't use docker
```

If the `.kube/config` file was not updated it can be populated using

`kind --name redis-test get kubeconfig > ~/.kube/config`

**2. Installing the operator**

After having a running `kind` cluster we should install the operator crd. All make commands should run from the repository root.

```bash
make install
```

After having a running `kind` cluster the operator can be deployed using

`make deploy NOTEST=true`

A development operator YAML file can be found in `redis-operator/config/samples/local_cluster.yaml`, apply it to the cluster after the operator deployment is up with:

`kubectl apply -f config/samples/local_cluster.yaml`

**Tip**

If you are missing an image in the cluster, an easy way to build and load all needed images is with the setup for E2E tests

`make e2e-test-setup`

### Deploy via Helm

The Helm chart was developed with Helm v3, it might work with v2 but it was not tested as such.
The chart can create the CRD, Redis operator and RedisCluster CR and it has feature flags for all of them plus the RBAC setup.

Examples:

```
# create everything in the default namespace
helm install redis-operator ./helm -n default

# install only the Redis operator in the default namespace
helm install redis-operator ./helm -n default --set global.rbac.create=false redisCluster.enabled=false --skip-crds

# install only the RBAC configuration (Role+Binding, ClusterRole+Binding, ServiceAccount)
helm install redis-operator-rbac ./helm -n default --set redisOperator=false redisCluster.enabled=false --skip-crds

#install only the RedisCluster in the default namespace
helm install redis-operator-rbac ./helm -n default --set redisOperator=false global.rbac.create=false --skip-crds
```

### Running the E2E tests

If you plan to make a contribution to the project please make sure the change is tested with the E2E test suite.
Check the README on how to use E2E tests: [https://github.com/PayU/redis-operator/blob/master/test/README.md](https://github.com/PayU/redis-operator/blob/master/test/README.md)

---

### Quick development using Telepresence

To develop directly on a deployed operator without rebuilding and loading/deploying the image you need to have access to a cluster (can be remote or local) and [Telepresence](https://www.telepresence.io/) tool.

<b>Note: If this is your first time using Telepresence you should make sure it has the right system permissions to work properly.</b><br>

1. Build the development image:

`make docker-build-dev NOTEST=true`

2. Swap the dev image with the operator deploy using Telepresence

```
telepresence --mount /tmp/podtoken  --context kind-redis-test --namespace default --swap-deployment redis-operator-manager --docker-run --env NAMESPACE_ENV=default --rm -it -v $(pwd):/app -v=/tmp/podtoken/var/run/secrets:/var/run/secrets redis-operator:dev
```

**How it works**

While the image used for production and local deployment is optimised for size, the development image does not discard the environment tools and allows the operator controller binary to be compiled inside the container. The image uses the CompileDaemon Go module in the ENTRYPOINT to track the source files, compile and build the controller. Every time the source files change a build and run are triggered.

The source code directory is mounted at `/app` and a special directory `/var/run/secrets` is used to load the permissions of the deployment that is being swapped. For more information check the Telepresence documentation [Link1](https://www.telepresence.io/tutorials/docker) & [Link2](https://www.telepresence.io/tutorials/kubernetes-client-libs).
