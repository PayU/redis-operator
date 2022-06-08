package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	dbv1 "github.com/PayU/redis-operator/api/v1"
	rediscli "github.com/PayU/redis-operator/controllers/rediscli"
	view "github.com/PayU/redis-operator/controllers/view"
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

func getSelectorRequirementFromPodLabelSelector(redisCluster *dbv1.RedisCluster) []metav1.LabelSelectorRequirement {
	lsr := []metav1.LabelSelectorRequirement{}
	for k, v := range redisCluster.Spec.PodLabelSelector {
		lsr = append(lsr, metav1.LabelSelectorRequirement{Key: k, Operator: metav1.LabelSelectorOpIn, Values: []string{v}})
	}
	return lsr
}

func (r *RedisClusterReconciler) getClusterStateView(redisCluster *dbv1.RedisCluster) error {
	configMapName := r.RedisClusterStateView.Name
	configMapNamespace := redisCluster.ObjectMeta.Namespace
	var configMap corev1.ConfigMap
	var redisClusterStateView view.RedisClusterStateView
	e := r.Get(context.Background(), client.ObjectKey{Name: configMapName, Namespace: configMapNamespace}, &configMap)
	if e != nil {
		return e
	}
	view := configMap.Data["data"]

	err := json.Unmarshal([]byte(view), &redisClusterStateView)
	if err != nil {
		return err
	}
	r.RedisClusterStateView = &redisClusterStateView
	return nil
}

// Update methods

func (r *RedisClusterReconciler) saveClusterStateView(redisCluster *dbv1.RedisCluster) {
	r.Log.Info("Saving cluster state view")
	configMapName := r.RedisClusterStateView.Name
	configMapNamespace := redisCluster.ObjectMeta.Namespace
	bytes, e := json.Marshal(r.RedisClusterStateView)
	if e != nil {
		r.Log.Error(e, "Error while attemting json marshal for cluster state view...")
		return
	}
	data := map[string]string{
		"data": string(bytes),
	}
	configMap := corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: configMapNamespace,
		},
		Data: data,
	}
	e = r.Update(context.Background(), &configMap)
	if e != nil {
		r.Log.Error(e, "Error while attemting update for cluster state view...")
		return
	}
	r.Log.Info("Cluster state view saved")
}

// Create/Make/Write methods

func (r *RedisClusterReconciler) postNewClusterStateView(redisCluster *dbv1.RedisCluster) error {
	configMapName := r.RedisClusterStateView.Name
	configMapNamespace := redisCluster.ObjectMeta.Namespace
	bytes, e := json.Marshal(r.RedisClusterStateView)
	if e != nil {
		return e
	}
	data := map[string]string{
		"data": string(bytes),
	}
	configMap := corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: configMapNamespace,
		},
		Data: data,
	}
	return r.Create(context.Background(), &configMap)
}

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

func (r *RedisClusterReconciler) makeAndCreateRedisPod(redisCluster *dbv1.RedisCluster, n *view.NodeStateView, createOpts []client.CreateOption) (corev1.Pod, error) {
	preferredLabelSelectorRequirement := []metav1.LabelSelectorRequirement{{Key: "leader-name", Operator: metav1.LabelSelectorOpIn, Values: []string{n.LeaderName}}}
	var role string
	if n.Name == n.LeaderName {
		role = "leader"
	} else {
		role = "follower"
	}
	pod := r.makeRedisPod(redisCluster, role, n.LeaderName, n.Name, preferredLabelSelectorRequirement)
	err := ctrl.SetControllerReference(redisCluster, &pod, r.Scheme)
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("Could not re create pod [%s]", n.Name))
		r.deletePod(pod)
		return pod, err
	}
	err = r.Create(context.Background(), &pod, createOpts...)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		r.Log.Error(err, fmt.Sprintf("Could not re create pod [%s]", n.Name))
		r.deletePod(pod)
		return pod, err
	}
	return pod, nil
}

func (r *RedisClusterReconciler) createMissingRedisPods(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) map[string]corev1.Pod {
	var pods []corev1.Pod
	createOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.NodeState == view.DeleteNode || n.NodeState == view.ReshardNode || n.NodeState == view.DeleteNodeKeepInMap || n.NodeState == view.ReshardNodeKeepInMap {
			continue
		}
		if _, isLeaderReported := r.RedisClusterStateView.Nodes[n.LeaderName]; !isLeaderReported {
			node, exists := v.Nodes[n.Name]
			if exists && node != nil {
				isMaster, err := r.checkIfMaster(node.Ip)
				if err != nil || !isMaster {
					r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.DeleteNode)
				} else {
					r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.ReshardNode)
				}
			} else {
				r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.DeleteNode)
			}
			continue
		}
		node, exists := v.Nodes[n.Name]
		if exists && node != nil {
			nodes, err := r.ClusterNodesWaitForRedisLoadDataSetInMemory(node.Ip)
			if err != nil || nodes == nil {
				r.RedisClusterStateView.LockResourceAndSetNodeState(n.Name, n.LeaderName, view.DeleteNodeKeepInMap, mutex)
				continue
			}
			if len(*nodes) == 1 {
				r.RedisClusterStateView.LockResourceAndSetNodeState(n.Name, n.LeaderName, view.AddNode, mutex)
				continue
			} else {
				n, inMap := r.RedisClusterStateView.Nodes[node.Name]
				if !inMap {
					r.RedisClusterStateView.LockResourceAndSetNodeState(n.Name, n.LeaderName, view.ReplicateNode, mutex)
				} else if n.NodeState != view.ReplicateNode && n.NodeState != view.SyncNode {
					r.RedisClusterStateView.LockResourceAndSetNodeState(n.Name, n.LeaderName, view.NodeOK, mutex)
				}
			}
			continue
		}
		wg.Add(1)
		go func(n *view.NodeStateView, wg *sync.WaitGroup) {
			mutex.Lock()
			defer wg.Done()
			mutex.Unlock()
			r.RedisClusterStateView.LockResourceAndSetNodeState(n.Name, n.LeaderName, view.CreateNode, mutex)
			pod, err := r.makeAndCreateRedisPod(redisCluster, n, createOpts)
			if err != nil {
				return
			}
			mutex.Lock()
			pods = append(pods, pod)
			mutex.Unlock()
		}(n, &wg)
	}
	wg.Wait()
	readyPods := map[string]corev1.Pod{}
	if len(pods) > 0 {
		newPods, err := r.waitForPodNetworkInterface(pods...)
		if err != nil {
			r.Log.Error(err, fmt.Sprintf("Could not re create missing pods"))
			r.deletePods(pods)
			return map[string]corev1.Pod{}
		}
		for _, p := range newPods {
			wg.Add(1)
			go func(p corev1.Pod, wg *sync.WaitGroup) {
				mutex.Lock()
				defer wg.Done()
				mutex.Unlock()
				readyPod, err := r.waitForRedisPod(p)
				if err != nil {
					r.deletePod(p)
					return
				}
				mutex.Lock()
				readyPods[readyPod.Name] = readyPod
				s := r.RedisClusterStateView.Nodes[readyPod.Name]
				r.RedisClusterStateView.SetNodeState(s.Name, s.LeaderName, view.AddNode)
				mutex.Unlock()
			}(p, &wg)
		}
		wg.Wait()
	}
	return readyPods
}

func (r *RedisClusterReconciler) waitForRedisPod(p corev1.Pod) (corev1.Pod, error) {
	r.Log.Info(fmt.Sprintf("Waiting for redis pod [%s]", p.Name))
	podArray, err := r.waitForPodReady(p)
	if err != nil || len(podArray) == 0 {
		r.Log.Error(err, fmt.Sprintf("Could not re create pod [%s]", p.Name))
		r.deletePod(p)
		return corev1.Pod{}, err
	}
	pod := podArray[0]
	err = r.waitForRedis(pod.Status.PodIP)
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("Could not re create pod [%s]", p.Name))
		r.deletePod(p)
		return corev1.Pod{}, err
	}
	return pod, nil
}

func (r *RedisClusterReconciler) createRedisLeaderPods(redisCluster *dbv1.RedisCluster, nodeNames ...string) ([]corev1.Pod, error) {
	if len(nodeNames) == 0 {
		return nil, errors.New("Failed to create leader pods - no node names")
	}
	var leaderPods []corev1.Pod
	for _, nodeName := range nodeNames {
		pod, err := r.makeLeaderPod(redisCluster, nodeName)
		if err != nil {
			return nil, err
		}
		err = ctrl.SetControllerReference(redisCluster, &pod, r.Scheme)
		if err != nil {
			r.Log.Error(err, fmt.Sprintf("Could not re create pod [%s]", pod.Name))
			r.deletePod(pod)
			return leaderPods, err
		}
		leaderPods = append(leaderPods, pod)
	}

	applyOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}
	for _, pod := range leaderPods {
		err := r.Create(context.Background(), &pod, applyOpts...)
		if err != nil && !apierrors.IsAlreadyExists(err) && !apierrors.IsConflict(err) {
			return nil, err
		}
	}
	newPods := []corev1.Pod{}
	var err error
	if len(leaderPods) > 0 {
		newPods, err = r.waitForPodNetworkInterface(leaderPods...)
		if err != nil {
			return nil, err
		}
		r.Log.Info(fmt.Sprintf("New leader pods created: %v", nodeNames))
	}
	return newPods, nil
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
		if pollErr := wait.PollImmediate(r.Config.Times.PodReadyCheckInterval, 4*r.Config.Times.PodReadyCheckTimeout, func() (bool, error) {
			err := r.Get(context.Background(), key, &pod)
			if err != nil {
				return true, err
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
		if pollErr := wait.PollImmediate(r.Config.Times.PodNetworkCheckInterval, r.Config.Times.PodNetworkCheckTimeout, func() (bool, error) {
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

func (r *RedisClusterReconciler) waitForPodDelete(pods ...corev1.Pod) {
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	for _, p := range pods {
		wg.Add(1)
		go func(p corev1.Pod, wg *sync.WaitGroup) {
			mutex.Lock()
			defer wg.Done()
			mutex.Unlock()
			key, err := client.ObjectKeyFromObject(&p)
			if err != nil {
				r.Log.Error(err, "Error while getting object key for deletion process")
				return
			}
			r.Log.Info(fmt.Sprintf("Waiting for pod delete: %s", p.Name))
			if pollErr := wait.Poll(r.Config.Times.PodDeleteCheckInterval, r.Config.Times.PodDeleteCheckTimeout, func() (bool, error) {
				err := r.Get(context.Background(), key, &p)
				if err != nil {
					if apierrors.IsNotFound(err) {
						return true, nil
					}
					return false, err
				}
				return false, nil
			}); pollErr != nil {
				r.Log.Error(err, "Error while waiting for pod to be deleted")
				return
			}
		}(p, &wg)
	}
	wg.Wait()
}

// Delete methods

func (r *RedisClusterReconciler) deleteAllRedisClusterPods() error {
	pods, e := r.getRedisClusterPods(cluster)
	if e != nil {
		return e
	}
	deletedPods, err := r.deletePods(pods)
	if err != nil {
		return err
	}
	r.waitForPodDelete(deletedPods...)
	return nil
}

func (r *RedisClusterReconciler) deletePods(pods []corev1.Pod) ([]corev1.Pod, error) {
	deletedPods := []corev1.Pod{}
	for _, pod := range pods {
		err := r.deletePod(pod)
		if err != nil {
			return deletedPods, err
		}
		deletedPods = append(deletedPods, pod)
	}
	return deletedPods, nil
}

func (r *RedisClusterReconciler) deletePod(pod corev1.Pod) error {
	if err := r.Delete(context.Background(), &pod); err != nil && apierrors.IsNotFound(err) {
		r.Log.Error(err, "Could not delete pod: "+pod.Name)
		return err
	}
	return nil
}

func (r *RedisClusterReconciler) deleteClusterStateView(redisCluster *dbv1.RedisCluster) error {
	configMapName := r.RedisClusterStateView.Name
	configMapNamespace := redisCluster.ObjectMeta.Namespace
	var configMap corev1.ConfigMap
	e := r.Get(context.Background(), client.ObjectKey{Name: configMapName, Namespace: configMapNamespace}, &configMap)
	if e != nil {
		return e
	}
	if len(configMap.Data) > 0 {
		return r.Delete(context.Background(), &configMap)
	}
	return nil
}

func (r *RedisClusterReconciler) ClusterNodesWaitForRedisLoadDataSetInMemory(ip string) (nodes *rediscli.RedisClusterNodes, err error) {
	if pollErr := wait.PollImmediate(2*time.Second, 10*time.Second, func() (bool, error) {
		nodes, _, err = r.RedisCLI.ClusterNodes(ip)
		if err != nil {
			if strings.Contains(err.Error(), "Redis is loading the dataset in memory") {
				return false, nil
			}
			return false, err
		}
		return true, err
	}); pollErr != nil {
		return nil, pollErr
	}

	return nodes, err
}
