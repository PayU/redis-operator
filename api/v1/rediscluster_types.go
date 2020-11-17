/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodLabelSelector will use to identify
// which pod our controller needs to maintain
type PodLabelSelector struct {
	App string `json:"app"`
}

// TopologyKeys deifines the topology keys used inside affinity rules
type TopologyKeys struct {
	HostTopologyKey string `json:"hostTopologyKey,omitempty"`
	ZoneTopologyKey string `json:"zoneTopologyKey,omitempty"`
}

// RedisClusterSpec defines the desired state of RedisCluster
type RedisClusterSpec struct {

	// +kubebuilder:validation:Minimum=3
	// The number of leader instances to run.
	LeaderReplicas int32 `json:"leaderReplicas,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=0
	// The number of followers that each leader will have
	LeaderFollowersCount int32 `json:"leaderFollowersCount,omitempty"`

	// +kubebuilder:validation:MinLength=2
	// full path of the redis docker image
	Image string `json:"image"`

	ImagePullSecrets string `json:"imagePullSecrets,omitempty"`

	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	PodResources corev1.ResourceRequirements `json:"Podresources,omitempty"`

	PodLabelSelector PodLabelSelector `json:"podLabelSelector"`

	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	Affinity TopologyKeys `json:"affinity,omitempty"`

	RedisContainerEnvVariables []corev1.EnvVar `json:"redisContainerEnvVariables,omitempty"`
}

// RedisClusterStatus defines the observed state of RedisCluster
type RedisClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// A list of pointers to currently running pods.
	// +optional
	Pods []corev1.ObjectReference `json:"active,omitempty"`

	// the current state of the cluster
	// +optional
	ClusterState string `json:"clusterState,omitempty"`

	// the total expected pod number when
	// the cluster is ready and stable
	// +optional
	TotalExpectedPods int32 `json:"totalExpectedPods,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=rdc

// RedisCluster is the Schema for the redisclusters API
type RedisCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedisClusterSpec   `json:"spec,omitempty"`
	Status RedisClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RedisClusterList contains a list of RedisCluster
type RedisClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RedisCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RedisCluster{}, &RedisClusterList{})
}
