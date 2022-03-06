CLUSTER_NAME="redis-test"
CONTEXT_NAME="kind-$CLUSTER_NAME"

echo "CLUSTER_NAME $CLUSTER_NAME"
echo "CONTEXT_NAME $CONTEXT_NAME"

echo $(kubectl config set-context $CONTEXT_NAME)
echo $(kubectl config use-context $CONTEXT_NAME)

python3 gen_kind_config.py
kind create cluster --name $CLUSTER_NAME --config cloud.yaml
kind --name $CLUSTER_NAME get kubeconfig > "$CLUSTER_NAME.kubeconfig.yaml"

current_context=$(kubectl config current-context)
echo "CURRENT CONTEXT $current_context"
if [ "$current_context" == "$CONTEXT_NAME" ]; then
  # add metrics server to the cluster
  kubectl apply -f addons/metrics-server.yaml
  kubectl patch deployment metrics-server -n kube-system -p '{"spec":{"template":{"spec":{"containers":[{"name":"metrics-server","args":["--cert-dir=/tmp", "--secure-port=4443", "--kubelet-insecure-tls","--kubelet-preferred-address-types=InternalIP"]}]}}}}'
else
  echo "Please set the current cluster context to $CONTEXT_NAME and re-run the install script"
fi
