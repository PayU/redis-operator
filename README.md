# PayU Redis Operator

Kubernetes operator that creates and manages Redis HA clustered databases - [Redis docs](https://redislabs.com/redis-enterprise/technology/redis-enterprise-cluster-architecture/).<br>
[ Feature state wiki page ](https://github.com/PayU/redis-operator/wiki/Feature-state)

### Using the operator on a local cluster

The project uses a [kind](https://kind.sigs.k8s.io/docs/user/quick-start/) cluster for development and test purposes.

Requirements:

* `kind`: [v0.11.1](https://github.com/kubernetes-sigs/kind/releases/tag/v0.11.1)
* `controller-gen` > 0.4
* `kustomize` >= 4.0
* `docker`: latest version, at least 6.25 GB of memory limit

**1. Setting up a cluster**

```bash
cd hack
sh ./install.sh # you might need to run this as sudo if a regular user can't use docker
```

If the `.kube/config` file was not updated it can be populated using

`kind --name redis-test get kubeconfig > ~/.kube/config-redis-operator`

And adding to `~/.zshrc` or `~/.bash_profile` the command `export KUBECONFIG=${HOME}/.kube/config-redis-operator`

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
helm install redis-operator ./helm -n default --set global.rbac.create=false --set redisCluster.enabled=false --skip-crds

# install only the RBAC configuration (Role+Binding, ClusterRole+Binding, ServiceAccount)
helm install redis-operator-rbac ./helm -n default --set redisOperator=false --set redisCluster.enabled=false --skip-crds

#install only the RedisCluster in the default namespace
helm install redis-operator-rbac ./helm -n default --set redisOperator=false --set global.rbac.create=false --skip-crds
```

### Running the E2E tests

If you plan to make a contribution to the project please make sure the change is tested with the E2E test suite.
Check the README on how to use E2E tests: [https://github.com/PayU/redis-operator/blob/master/test/README.md](https://github.com/PayU/redis-operator/blob/master/test/README.md)

### Running unit tests

Run non cache `go test` command on specific path. for example:
```
go test -count=1 ./controllers/rediscli/
```

### Development using Tilt

The recommended development flow is based on [Tilt](https://tilt.dev/) - it is used for quick iteration on code running in live containers.
Setup based on [official docs](https://docs.tilt.dev/example_go.html) can be found in the Tiltfile.

Prerequisites:

1. Install the Tilt tool
2. Build the redis-cli binary: go to `hack/redis-bin` and run the `run.sh` script. It will build the redis-cli binary inside a Linux container so it can be used by Tilt when building the dev image. You only need to do this once. Check that you have redis-cli in `hack/redis-bin/build` directory.
3. Run `tilt up` and go the indicated localhost webpage

```
> tilt up
Tilt started on http://localhost:10350/
v0.22.15, built 2021-10-29

(space) to open the browser
(s) to stream logs (--stream=true)
(t) to open legacy terminal mode (--legacy=true)
(ctrl-c) to exit
```

---

### Development using Telepresence

*The Telepresence setup is deprecated, it only works with telepresence V1 and will be remvoved once the Tilt flow is fully adopted.*

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
