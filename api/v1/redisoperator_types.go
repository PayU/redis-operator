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

// RedisOperatorSpec defines the desired state of RedisOperator
type RedisOperatorSpec struct {

	// +kubebuilder:validation:Minimum=3
	// The number of leader instances to run.
	LeaderReplicas int32 `json:"leaderReplicas,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=0
	// The number of followers that each leader will have
	LeaderFollowersCount *int32 `json:"leaderFollowersCount,omitempty"`

	// +kubebuilder:validation:MinLength=2
	// full path of the redis docker image
	Image string `json:"image"`

	ImagePullSecrets string `json:"imagePullSecrets,omitempty"`

	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	PodResources corev1.ResourceRequirements `json:"Podresources,omitempty"`

	PodLabelSelector PodLabelSelector `json:"podLabelSelector"`

	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	RedisContainerEnvVariables []corev1.EnvVar `json:"redisContainerEnvVariables,omitempty"`
}

// RedisOperatorStatus defines the observed state of RedisOperator
type RedisOperatorStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// A list of pointers to currently running pods.
	// +optional
	Pods []corev1.ObjectReference `json:"active,omitempty"`

	// the current state of the cluster
	// +optional
	ClusterState string `json:"clusterState,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=ro

// RedisOperator is the Schema for the redisoperators API
type RedisOperator struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedisOperatorSpec   `json:"spec,omitempty"`
	Status RedisOperatorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RedisOperatorList contains a list of RedisOperator
type RedisOperatorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RedisOperator `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RedisOperator{}, &RedisOperatorList{})
}
