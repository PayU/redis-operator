load('ext://restart_process', 'docker_build_with_restart')

if k8s_context() != 'kind-redis-test':
  fail('Expected K8s context to be "kind-redis-cluster", found: ' + k8s_context())

# local_resource('build-redis', './hack/redis-bin/run.sh')

compile_cmd = 'CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -o bin/manager main.go'
local_resource(
  'redis-operator-compile',
  compile_cmd,
  deps=['./main.go', './api', './controllers', './server']
)

docker_build(
  ref='metrics-exporter:testing',
  context='./hack',
  dockerfile='./hack/metrics-exporter.Dockerfile',
)

docker_build(
  ref='redis-init:testing',
  context='./hack',
  dockerfile='./hack/redis-init.Dockerfile',
)

docker_build(
  ref='redis:testing',
  context='./hack',
  dockerfile='./hack/redis.Dockerfile',
)

docker_build_with_restart(
  'redis-operator-docker',
  '.',
  entrypoint='/manager',
  ignore=['./bin/manager-go-tmp-umask'],
  dockerfile='tilt.Dockerfile',
  only=[
    './hack/redis-bin/build/redis-cli',
    './bin',
  ],
  live_update=[
    sync('./bin/manager', '/manager'),
  ],
)

k8s_yaml('./helm/crds/crd-rediscluster.yaml')
k8s_yaml(local('helm template ./helm --name-template=redis-operator -n default --set redisOperator.managerReplicas=1'))

k8s_resource(
  new_name='redis-cluster-crd',
  objects=['redisclusters.db.payu.com:customresourcedefinition'],
)

k8s_kind('RedisCluster',
  image_json_path=[
    '{.spec.redisPodSpec.containers[0].image}',
    '{.spec.redisPodSpec.containers[1].image}',
    '{.spec.redisPodSpec.initContainers[0].image}',
])
# k8s_resource(
#   new_name='redis-cluster',
#   objects=['dev-rdc:rediscluster'],
#   extra_pod_selectors=[{'redis-cluster': 'dev-rdc'}],
#   resource_deps=['redis-cluster-crd'],
# )

k8s_resource(
  new_name='redis-operator-config',
  objects=[
    'redis-operator-manager:serviceaccount',
    'redis-operator:role', 'redis-operator:rolebinding',
    'redis-operator:clusterrole', 'redis-operator:clusterrolebinding',
    'dev-rdc-users-acl:configmap',
    'dev-rdc-redisconfig:configmap',
    'redis-operator-config:configmap',],
)

k8s_resource(workload='redis-operator-manager', port_forwards=[
    port_forward(8080, 8080, "api-server"),
    port_forward(8081, 9808, "metrics"),],
  resource_deps=['redis-cluster-crd', 'redis-operator-config', 'redis-operator-compile']
)
