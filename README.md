How to update the crd:

`make install`

How to update the manager/controller code:

`make manager`

How to deploy the controller:

`make deploy`

For testing the Redis operator on a local cluster:

```bash
cd testing
python gen_kind_config.py # creates a new kind cluster config file
sh ./install.sh # you might need to run this as sudo if a regular user can't use docker
```

If the `.kube/config` file was not update it can be populated using

`kind --name redis-test get kubeconfig > ~/.kube/config`

After having a running cluster the operator can be deployed using

`make deploy-local DEV=true`

