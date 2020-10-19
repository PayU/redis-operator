How to update the crd:

`make install`

How to update the manager/controller code:

`make manager`

How to deploy the controller to a cluster (using the current context):

`make deploy`

---

### Using a local cluster

For testing the Redis operator on a local `kind` cluster:

```bash
cd hack
python gen_kind_config.py # creates a new kind cluster config file
sh ./install.sh # you might need to run this as sudo if a regular user can't use docker
```

If the `.kube/config` file was not updated it can be populated using

`kind --name redis-test get kubeconfig > ~/.kube/config`

After having a running `kind` cluster we should install the operator crd.

```bash
 cd ..
 make install
```

After having a running `kind` cluster the operator can be deployed using

`make deploy LOCAL=true`

A development operator YAML file can be found in `Redis-Operator/config/samples/local_cluster.yaml`, apply it to the cluster after the operator deployment is up with:

`kubectl apply -f config/samples/local_cluster.yaml`

---

### Quick development using Telepresence

To develop directly on a deployed operator without rebuilding and loading/deploying the image you need to have access to a cluster (can be remote or local) and [Telepresence](https://www.telepresence.io/) tool.

1. Build the development image:

`make docker-build-dev`

2. Swap the dev image with the operator deploy using Telepresence

```
telepresence --mount /tmp/podtoken  --context kind-redis-test --namespace logs --swap-deployment redis-operator-controller-manager --docker-run --rm -it -v $(pwd):/app -v=/tmp/podtoken/var/run/secrets:/var/run/secrets redis-operator:dev
```

**How it works**

While the image used for production and local deployment is optimised for size, the development image does not discard the environment tools and allows the operator controller binary to be compiled inside the container. The image uses the CompileDaemon Go module in the ENTRYPOINT to track the source files, compile and build the controller. Every time the source files change a build and run are triggered.

The source code directory is mounted at `/app` and a special directory `/var/run/secrets` is used to load the permissions of the deployment that is being swapped. For more information check the Telepresence documentation [Link1](https://www.telepresence.io/tutorials/docker) & [Link2](https://www.telepresence.io/tutorials/kubernetes-client-libs).
