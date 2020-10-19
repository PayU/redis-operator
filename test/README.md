## E2E Testing

End-to-end automated testing for the Redis cluster.

---

### How it works

The tests use a framework that has features for setting up the necessary K8s resources and for interactig with a cluster. It runs kustomize to generate the K8s resources.

### How to run tests

**Prepare the kind cluster**

If you don't already have a Kubernetes cluster running check the steps from the repository readme to create one. The following steps assume a local kind cluster is up and running.

The e2e test can be run with the `e2e-test` makefile target, it will first build the required images and load them in kind.

```
make e2e-test
```

`go test` can be used directly to avoid rebuilding the images and just run the test code

```
cd test/e2e
go test -tags=e2e_redis_op
```

TODO

---

### Developing tests

The testsuite and framework are kept behind a build tag and are only compiled and included in the binary if the tag is provided.

If using Neovim with CoC `gopls` checks can be enabled from `coc-settings.json` with:

```
{
  "go.goplsEnv": {
    "GOFLAGS": "-tags=e2e_redis_op"
  }
}
```

---

Framework structure inspired by `prometheus-operator` [testing framework](https://github.com/prometheus-operator/prometheus-operator/tree/master/test).
