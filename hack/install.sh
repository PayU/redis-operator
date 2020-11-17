CLUSTER_NAME=redis-test

python gen_kind_config.py
kind create cluster --name $CLUSTER_NAME --config cloud.yaml
kind --name $CLUSTER_NAME get kubeconfig > kubeconfig.yaml

current_context=$(kubectl config current-context)
if [ "$current_context" = "kind-$CLUSTER_NAME" ]; then
  kubectl create -f ns.yaml
else
  echo "Please set the current cluster config to kind-redis-test and run"
  echo "kubectl create -f ns.yaml"
fi
