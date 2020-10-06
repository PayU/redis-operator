python gen_kind_config.py
kind create cluster --name redis-test --config cloud.yaml

current_context=$(kubectl config current-context)
if [ $current_context == "kind-redis-test" ]; then
  kubectl create -f ns.yaml
else
  echo "Please set the current cluster config to kind-redis-test and run\n"
  echo "kubectl create -f ns.yaml"
fi
