## E2E Testing

End-to-end automated testing for the Redis cluster.
For pre-requisites check the main README file. In addition, the tests will expect the `redis-cli` binary to be in PATH`.

---

### How it works

The tests use a framework that has features for setting up the necessary K8s resources and for interacting with a cluster. It runs kustomize to generate the K8s resources.

### How to run tests

**Prepare the kind cluster**

If you don't already have a Kubernetes cluster running check the steps from the repository readme to create one. The following steps assume a local kind cluster is up and running.

1. Build images for the operator and Redis

```
make e2e-test-setup
```

2. Run the e2e tests

```
cd test/e2e
go test -tags=e2e_redis_op -count=1
```

*It is important to add the `-count=1` flag otherwise the test resutls will be cached between multiple runs with unexpected results*

If tests take longer than 10 minutes it could timeout by default. A larger timeout can be provided with the `-timeout` flag

```
go test -tags=e2e_redis_op -count=1 -timeout=20m
```

---

### Developing tests

The test suite and framework are kept behind a build tag and are only compiled and included in the binary if the tag is provided.

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
