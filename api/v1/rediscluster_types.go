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

// RedisClusterSpec defines the desired state of RedisCluster.
type RedisClusterSpec struct {

	// +kubebuilder:validation:Minimum=3
	// The number of leader instances to run.
	LeaderCount int `json:"leaderCount,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=0
	// The number of followers that each leader will have.
	LeaderFollowersCount int `json:"leaderFollowersCount,omitempty"`

	// +optional
	// Flag that toggles the default affinity rules added by the operator.
	// Default is true.
	EnableDefaultAffinity bool `json:"enableDefaultAffinity,omitempty"`

	// +optional
	// Annotations for the Redis pods.
	Annotations map[string]string `json:"annotations,omitempty"`

	// Labels used by the operator to get the pods that it manages. Added by default
	// to the list of labels of the Redis pod.
	PodLabelSelector map[string]string `json:"podLabelSelector"`

	// +optional
	// Labels for the Redis pods.
	Labels map[string]string `json:"labels,omitempty"`

	// PodSpec for Redis pods.
	RedisPodSpec corev1.PodSpec `json:"redisPodSpec"`
}

// RedisClusterStatus defines the observed state of RedisCluster
type RedisClusterStatus struct {
	// A list of pointers to currently running pods.
	// +optional
	Pods []corev1.ObjectReference `json:"active,omitempty"`

	// The current state of the cluster.
	// +optional
	ClusterState string `json:"clusterState,omitempty"`

	// The total expected pod number when the cluster is ready and stable.
	// +optional
	TotalExpectedPods int `json:"totalExpectedPods,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=rdc

// RedisCluster is the Schema for the redisclusters API.
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
