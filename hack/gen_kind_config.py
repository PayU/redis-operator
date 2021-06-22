from enum import Enum

class ClusterParams:
    CONFIG_FILENAME = 'cloud.yaml'
    NODE_LAYOUT = [['eu-central-1a']*3, ['eu-central-1b']*3, ['eu-central-1c']*3]
    ZONE_KEY = 'failure-domain.beta.kubernetes.io/zone'
    KIND_API_VERSION = 'kind.x-k8s.io/v1alpha4'
    KUBERNETES_VERSION = 'v1.16.15@sha256:83067ed51bf2a3395b24687094e283a7c7c865ccc12a8b1d7aa673ba0c5e8861'

def get_node_count():
    return len([zone for group in ClusterParams.NODE_LAYOUT for zone in group])

def get_node_configs():
    layout = [zone for group in ClusterParams.NODE_LAYOUT for zone in group]
    configs = []
    for zone in layout:
        configs.append(f'''
  image: kindest/node:{ClusterParams.KUBERNETES_VERSION}
  kubeadmConfigPatches:
  - |
    kind: JoinConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "{ClusterParams.ZONE_KEY}={zone}"
''')
    return configs

def get_config_header():
    return f'''kind: Cluster
apiVersion: {ClusterParams.KIND_API_VERSION}
nodes:
- role: control-plane
  image: kindest/node:{ClusterParams.KUBERNETES_VERSION}
'''

def gen_kind_config():
    print('Generating the cluster kind config...', end='')
    with open(ClusterParams.CONFIG_FILENAME, 'w') as fh:
        fh.write(get_config_header())
        patches = get_node_configs()
        for i in range(get_node_count()):
            fh.write('- role: worker')
            fh.write(patches[i])
    print('Done')


def main():
    gen_kind_config()


if __name__ == '__main__':
  main()

