package controllers

import (
	"context"
	"fmt"
	"strings"
	"sync"

	dbv1 "github.com/PayU/redis-operator/api/v1"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type K8sManager struct {
	client.Client
	Log    logr.Logger
	Config *RedisOperatorConfig
	Scheme *runtime.Scheme
}

// Get/Read methods

func (r *RedisClusterReconciler) getRedisClusterPods(redisCluster *dbv1.RedisCluster, podType ...string) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	matchingLabels := redisCluster.Spec.PodLabelSelector

	if len(podType) > 0 && strings.TrimSpace(podType[0]) != "" {
		pt := strings.TrimSpace(podType[0])
		if pt == "follower" || pt == "leader" {
			matchingLabels["redis-node-role"] = pt
		}
	}

	err := r.List(context.Background(), pods, client.InNamespace(redisCluster.ObjectMeta.Namespace), client.MatchingLabels(matchingLabels))
	if err != nil {
		return nil, err
	}

	return pods.Items, nil
}

func (r *RedisClusterReconciler) getRedisClusterPodsByLabel(redisCluster *dbv1.RedisCluster, key string, value string) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	matchingLabels := redisCluster.Spec.PodLabelSelector

	matchingLabels[key] = value

	err := r.List(context.Background(), pods, client.InNamespace(redisCluster.ObjectMeta.Namespace), client.MatchingLabels(matchingLabels))
	if err != nil {
		return nil, err
	}

	return pods.Items, nil
}

func (r *RedisClusterReconciler) getPodByIP(namespace string, podIP string) (corev1.Pod, error) {
	var podList corev1.PodList
	err := r.List(context.Background(), &podList, client.InNamespace(namespace), client.MatchingFields{"status.podIP": podIP})
	if err != nil {
		return corev1.Pod{}, err
	}
	if len(podList.Items) == 0 {
		return corev1.Pod{}, apierrors.NewNotFound(corev1.Resource("Pod"), "")
	}
	return podList.Items[0], nil
}

func getSelectorRequirementFromPodLabelSelector(redisCluster *dbv1.RedisCluster) []metav1.LabelSelectorRequirement {
	lsr := []metav1.LabelSelectorRequirement{}
	for k, v := range redisCluster.Spec.PodLabelSelector {
		lsr = append(lsr, metav1.LabelSelectorRequirement{Key: k, Operator: metav1.LabelSelectorOpIn, Values: []string{v}})
	}
	return lsr
}

func (r *RedisClusterReconciler) GetExpectedView(redisCluster *dbv1.RedisCluster) (map[string]string, error) {
	configMapName := r.ClusterStatusMapName
	configMapNamespace := redisCluster.ObjectMeta.Namespace
	var configMap corev1.ConfigMap
	err := r.Get(context.Background(), client.ObjectKey{Name: configMapName, Namespace: configMapNamespace}, &configMap)
	return configMap.Data, err
}

// Update methods

func (r *RedisClusterReconciler) UpdateExpectedView(redisCluster *dbv1.RedisCluster) error {
	configMapName := r.ClusterStatusMapName
	configMapNamespace := redisCluster.ObjectMeta.Namespace
	var configMap corev1.ConfigMap
	r.Get(context.Background(), client.ObjectKey{Name: configMapName, Namespace: configMapNamespace}, &configMap)
	if len(configMap.Data) > 0 {
		e := r.applyViewToConfigMap(redisCluster, &configMap)
		if e != nil {
			return e
		}
		return r.Update(context.Background(), &configMap, &client.UpdateOptions{})
	}
	// If not exists
	configMap = corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: configMapNamespace,
		},
		Data: nil,
	}
	e := r.applyViewToConfigMap(redisCluster, &configMap)
	if e != nil {
		return e
	}
	return r.Create(context.Background(), &configMap)
}

func (r *RedisClusterReconciler) applyViewToConfigMap(redisCluster *dbv1.RedisCluster, cm *corev1.ConfigMap) error {
	pods, err := r.getRedisClusterPods(redisCluster)
	if err != nil {
		return err
	}
	data := make(map[string]string)
	for _, p := range pods {
		data[p.Name] = p.Labels["leader-name"]
	}
	cm.Data = data
	return nil
}

// Create/Make/Write methods

func (r *K8sManager) WritePodAnnotations(annotations map[string]string, pods ...corev1.Pod) error {
	annotationsString := ""
	for key, val := range annotations {
		annotationsString = fmt.Sprintf("\"%s\": \"%s\",%s", key, val, annotationsString)
	}
	patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%s}}}`, annotationsString[:len(annotationsString)-1]))
	for _, pod := range pods {
		if err := r.Patch(context.Background(), &pod, client.RawPatch(types.StrategicMergePatchType, patch)); err != nil {
			r.Log.Error(err, fmt.Sprintf("Failed to patch the annotations on pod %s (%s)", pod.Name, pod.Status.PodIP))
		}
	}
	return nil
}

func (r *RedisClusterReconciler) makeRedisPod(redisCluster *dbv1.RedisCluster, nodeRole string, leaderName string, nodeName string, preferredLabelSelectorRequirement []metav1.LabelSelectorRequirement) corev1.Pod {
	var affinity corev1.Affinity
	podLabels := make(map[string]string)

	for k, v := range redisCluster.Spec.Labels {
		podLabels[k] = v
	}
	for k, v := range redisCluster.Spec.PodLabelSelector {
		podLabels[k] = v
	}

	podLabels["redis-node-role"] = nodeRole
	podLabels["leader-name"] = leaderName
	podLabels["node-name"] = nodeName
	podLabels["redis-cluster"] = redisCluster.Name

	if redisCluster.Spec.EnableDefaultAffinity {
		if redisCluster.Spec.RedisPodSpec.Affinity == nil {
			affinity = corev1.Affinity{}
		} else {
			affinity = *redisCluster.Spec.RedisPodSpec.Affinity
		}

		if affinity.PodAntiAffinity == nil {
			affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
		}

		requiredPodAffinityTerm := corev1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{
				MatchExpressions: getSelectorRequirementFromPodLabelSelector(redisCluster),
			},
			TopologyKey: "failure-domain.beta.kubernetes.io/node",
		}

		if affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
			affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{requiredPodAffinityTerm}
		} else {
			affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, requiredPodAffinityTerm)
		}

		preferredPodAffinityTerm := corev1.WeightedPodAffinityTerm{
			Weight: 100,
			PodAffinityTerm: corev1.PodAffinityTerm{
				LabelSelector: &metav1.LabelSelector{
					MatchExpressions: preferredLabelSelectorRequirement,
				},
				TopologyKey: "failure-domain.beta.kubernetes.io/zone",
			},
		}

		if affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution == nil {
			affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.WeightedPodAffinityTerm{preferredPodAffinityTerm}
		} else {
			affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, preferredPodAffinityTerm)
		}
	}

	spec := redisCluster.Spec.RedisPodSpec.DeepCopy()
	spec.Affinity = &affinity

	pod := corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        nodeName,
			Namespace:   redisCluster.ObjectMeta.Namespace,
			Labels:      podLabels,
			Annotations: redisCluster.Annotations,
		},
		Spec: *spec,
	}

	return pod
}

func (r *RedisClusterReconciler) makeFollowerPod(redisCluster *dbv1.RedisCluster, nodeName string, leaderName string) (corev1.Pod, error) {
	preferredLabelSelectorRequirement := []metav1.LabelSelectorRequirement{{Key: "leader-name", Operator: metav1.LabelSelectorOpIn, Values: []string{leaderName}}}
	pod := r.makeRedisPod(redisCluster, "follower", leaderName, nodeName, preferredLabelSelectorRequirement)

	if err := ctrl.SetControllerReference(redisCluster, &pod, r.Scheme); err != nil {
		return pod, err
	}

	return pod, nil
}

func (r *RedisClusterReconciler) createRedisService(redisCluster *dbv1.RedisCluster) (*corev1.Service, error) {
	svc, err := r.makeService(redisCluster)
	if err != nil {
		return nil, err
	}
	err = r.Create(context.Background(), &svc)
	if !apierrors.IsAlreadyExists(err) {
		return nil, err
	}
	return &svc, nil
}

func (r *RedisClusterReconciler) makeService(redisCluster *dbv1.RedisCluster) (corev1.Service, error) {
	service := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-cluster-service",
			Namespace: redisCluster.ObjectMeta.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:       "redis-client-port",
					Port:       6379,
					TargetPort: intstr.FromInt(6379),
				},
			},
			Selector: redisCluster.Spec.PodLabelSelector,
		},
	}

	if err := ctrl.SetControllerReference(redisCluster, &service, r.Scheme); err != nil {
		return service, err
	}

	return service, nil
}

func (r *RedisClusterReconciler) createRedisLeaderPods(redisCluster *dbv1.RedisCluster, nodeNames ...string) ([]corev1.Pod, error) {
	fmt.Println("Creating new leader...")
	if len(nodeNames) == 0 {
		return nil, errors.New("Failed to create leader pods - no node names")
	}

	var leaderPods []corev1.Pod
	for _, nodeName := range nodeNames {
		fmt.Println("Making pod " + nodeName)
		pod, err := r.makeLeaderPod(redisCluster, nodeName)
		if err != nil {
			return nil, err
		}
		leaderPods = append(leaderPods, pod)
	}

	applyOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}

	for _, pod := range leaderPods {
		fmt.Println("Creating pod with options")
		err := r.Create(context.Background(), &pod, applyOpts...)
		if err != nil && !apierrors.IsAlreadyExists(err) && !apierrors.IsConflict(err) {
			fmt.Printf(err.Error())
			return nil, err
		}
	}
	fmt.Println("Waiting for interface")
	leaderPods, err := r.waitForPodNetworkInterface(leaderPods...)
	if err != nil {
		return nil, err
	}

	r.Log.Info(fmt.Sprintf("New leader pods created: %v ", nodeNames))
	return leaderPods, nil
}

func (r *RedisClusterReconciler) makeLeaderPod(redisCluster *dbv1.RedisCluster, nodeName string) (corev1.Pod, error) {
	preferredLabelSelectorRequirement := []metav1.LabelSelectorRequirement{{Key: "redis-node-role", Operator: metav1.LabelSelectorOpIn, Values: []string{"leader"}}}
	pod := r.makeRedisPod(redisCluster, "leader", nodeName, nodeName, preferredLabelSelectorRequirement)

	if err := ctrl.SetControllerReference(redisCluster, &pod, r.Scheme); err != nil {
		return pod, err
	}
	return pod, nil
}

// Wait methods

func (r *RedisClusterReconciler) waitForPodReady(pods ...corev1.Pod) ([]corev1.Pod, error) {
	var readyPods []corev1.Pod
	for _, pod := range pods {
		key, err := client.ObjectKeyFromObject(&pod)
		if err != nil {
			return nil, err
		}
		r.Log.Info(fmt.Sprintf("Waiting for pod ready: %s(%s)", pod.Name, pod.Status.PodIP))
		if pollErr := wait.PollImmediate(2*r.Config.Times.PodReadyCheckInterval, 10*r.Config.Times.PodReadyCheckTimeout, func() (bool, error) {
			err := r.Get(context.Background(), key, &pod)
			if err != nil {
				return false, err
			}
			if pod.Status.Phase != corev1.PodRunning {
				return false, nil
			}
			for _, condition := range pod.Status.Conditions {
				if condition.Status != corev1.ConditionTrue {
					return false, nil
				}
			}
			readyPods = append(readyPods, pod)
			return true, nil
		}); pollErr != nil {
			return nil, pollErr
		}
	}
	return readyPods, nil
}

func (r *RedisClusterReconciler) waitForPodNetworkInterface(pods ...corev1.Pod) ([]corev1.Pod, error) {
	r.Log.Info(fmt.Sprintf("Waiting for pod network interfaces..."))
	var readyPods []corev1.Pod
	for _, pod := range pods {
		key, err := client.ObjectKeyFromObject(&pod)
		if pollErr := wait.PollImmediate(2*r.Config.Times.PodNetworkCheckInterval, 20*r.Config.Times.PodNetworkCheckTimeout, func() (bool, error) {
			if err = r.Get(context.Background(), key, &pod); err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				return false, err
			}
			if pod.Status.PodIP == "" {
				return false, nil
			}
			readyPods = append(readyPods, pod)
			return true, nil
		}); pollErr != nil {
			return nil, pollErr
		}
	}
	return readyPods, nil
}

func (r *RedisClusterReconciler) waitForPodDelete(pods ...corev1.Pod) error {
	for _, p := range pods {
		key, err := client.ObjectKeyFromObject(&p)
		if err != nil {
			return err
		}
		r.Log.Info(fmt.Sprintf("Waiting for pod delete: %s", p.Name))
		if pollErr := wait.Poll(2*r.Config.Times.PodDeleteCheckInterval, 5*r.Config.Times.PodDeleteCheckTimeout, func() (bool, error) {
			err := r.Get(context.Background(), key, &p)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				return false, err
			}
			return false, nil
		}); pollErr != nil {
			return pollErr
		}
	}
	return nil
}

// Delete methods

func (r *RedisClusterReconciler) deleteAllRedisClusterPods() error {
	pods, e := r.getRedisClusterPods(cluster)
	if e != nil {
		return e
	}
	var wg sync.WaitGroup
	wg.Add(len(pods))
	for _, pod := range pods {
		go r.deletePodAsync(pod.Namespace, pod.Status.PodIP, &wg)
	}
	wg.Wait()
	return nil
}

func (r *RedisClusterReconciler) deletePodAsync(namespace string, ip string, wg *sync.WaitGroup) {
	defer wg.Done()
	deletedPods, _ := r.deletePodsByIP(namespace, ip)
	if len(deletedPods) > 0 {
		r.waitForPodDelete(deletedPods...)
	}
}

func (r *RedisClusterReconciler) deletePodsByIP(namespace string, ip ...string) ([]corev1.Pod, error) {
	var deletedPods []corev1.Pod
	for _, ip := range ip {
		pod, err := r.getPodByIP(namespace, ip)
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		if err := r.Delete(context.Background(), &pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		deletedPods = append(deletedPods, pod)
	}
	return deletedPods, nil
}

func (r *RedisClusterReconciler) DeleteExpectedView(redisCluster *dbv1.RedisCluster) error {
	configMapName := r.ClusterStatusMapName
	configMapNamespace := redisCluster.ObjectMeta.Namespace
	var configMap corev1.ConfigMap
	err := r.Get(context.Background(), client.ObjectKey{Name: configMapName, Namespace: configMapNamespace}, &configMap)
	if err == nil && len(configMap.Data) > 0 {
		return r.Delete(context.Background(), &configMap)
	}
	return err
}
