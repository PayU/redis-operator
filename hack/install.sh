CLUSTER_NAME=redis-test

python3 gen_kind_config.py
kind create cluster --name $CLUSTER_NAME --config cloud.yaml
kind --name $CLUSTER_NAME get kubeconfig > "$CLUSTER_NAME.kubeconfig.yaml"

current_context=$(kubectl config current-context)
if [ "$current_context" = "kind-$CLUSTER_NAME" ]; then
  # add metrics server to the cluster
  kubectl apply -f addons/metrics-server.yaml
  kubectl patch deployment metrics-server -n kube-system -p '{"spec":{"template":{"spec":{"containers":[{"name":"metrics-server","args":["--cert-dir=/tmp", "--secure-port=4443", "--kubelet-insecure-tls","--kubelet-preferred-address-types=InternalIP"]}]}}}}'
else
  echo "Please set the current cluster context to kind-$CLUSTER_NAME and re-run the install script"
fi

# increasing inotify max users in order to aviod 'kind' too many open files errors
# more info can be found here: https://github.com/kubernetes-sigs/kind/issues/2586
KIND_DOCKER_IDS=$(docker ps -a -q)
KIND_DOCKER_IDS_ARRAY=($KIND_DOCKER_IDS)

for dockerID in "${KIND_DOCKER_IDS_ARRAY[@]}"
do
  :
  export dockerName=$(docker inspect $dockerID | jq .[0].Name)
  if [[ "$dockerName" == *"redis-test"* ]]; then
      echo "increase inotify max users for docker: $dockerName"
      docker exec -t $dockerID bash -c "echo 'fs.inotify.max_user_watches=1048576' >> /etc/sysctl.conf" 
      docker exec -t $dockerID bash -c "echo 'fs.inotify.max_user_instances=512' >> /etc/sysctl.conf"
      docker exec -i $dockerID bash -c "sysctl -p /etc/sysctl.conf"
  fi

done