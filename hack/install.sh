CLUSTER_NAME=redis-test
CALICO_VERSION=v3.17

python3 gen_kind_config.py
kind create cluster --name $CLUSTER_NAME --config cloud.yaml
kind --name $CLUSTER_NAME get kubeconfig > "$CLUSTER_NAME.kubeconfig.yaml"

current_context=$(kubectl config current-context)
if [ "$current_context" = "kind-$CLUSTER_NAME" ]; then
  kubectl create -f ns.yaml
  # install the Calico CNI
  kubectl apply -f "https://docs.projectcalico.org/$CALICO_VERSION/manifests/calico.yaml"
  kubectl -n kube-system set env daemonset/calico-node FELIX_IGNORELOOSERPF=true
  # add metrics server to the cluster
  kubectl apply -f addons/metrics-server.yaml
  kubectl patch deployment metrics-server -n kube-system -p '{"spec":{"template":{"spec":{"containers":[{"name":"metrics-server","args":["--cert-dir=/tmp", "--secure-port=4443", "--kubelet-insecure-tls","--kubelet-preferred-address-types=InternalIP"]}]}}}}'
else
  echo "Please set the current cluster context to kind-$CLUSTER_NAME and re-run the install script"
fi
