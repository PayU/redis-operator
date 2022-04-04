package controllers

import (
	"context"
	"fmt"
	"strings"

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

type RedisPodsView struct {
	PodViewByIp    map[string]*RedisPodView
	PodNameToPodIp map[string]string
}

type RedisPodView struct {
	Name            string
	Namespace       string
	IP              string
	Phase           string
	LeaderName      string
	IsLeader        bool
	FollowersByName []string
	Labels          map[string]string
	Pod             corev1.Pod
}

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

func getSelectorRequirementFromPodLabelSelector(redisCluster *dbv1.RedisCluster) []metav1.LabelSelectorRequirement {
	lsr := []metav1.LabelSelectorRequirement{}
	for k, v := range redisCluster.Spec.PodLabelSelector {
		lsr = append(lsr, metav1.LabelSelectorRequirement{Key: k, Operator: metav1.LabelSelectorOpIn, Values: []string{v}})
	}
	return lsr
}

func (r *RedisClusterReconciler) makeRedisPod(redisCluster *dbv1.RedisCluster, nodeRole string, leaderName string, nodeName string, preferredLabelSelectorRequirement []metav1.LabelSelectorRequirement, isLeader bool) corev1.Pod {
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

func (r *RedisClusterReconciler) makeFollowerPod(redisCluster *dbv1.RedisCluster, leaderName string, followerName string) (corev1.Pod, error) {
	preferredLabelSelectorRequirement := []metav1.LabelSelectorRequirement{{Key: "leader-name", Operator: metav1.LabelSelectorOpIn, Values: []string{leaderName}}}
	pod := r.makeRedisPod(redisCluster, "follower", leaderName, followerName, preferredLabelSelectorRequirement, false)

	if err := ctrl.SetControllerReference(redisCluster, &pod, r.Scheme); err != nil {
		return pod, err
	}

	return pod, nil
}

func (r *RedisClusterReconciler) createRedisFollowerPods(redisCluster *dbv1.RedisCluster, nodesData []NodeCreationData) ([]corev1.Pod, error) {
	var followerPods []corev1.Pod
	createOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}

	for _, d := range nodesData {
		pod, err := r.makeFollowerPod(redisCluster, d.LeaderName, d.NodeName)
		if err != nil {
			return nil, err
		}
		err = r.Create(context.Background(), &pod, createOpts...)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
		followerPods = append(followerPods, pod)
	}

	followerPods, err := r.waitForPodNetworkInterface(followerPods...)
	if err != nil {
		return nil, err
	}

	r.Log.Info(fmt.Sprintf("New follower pods created: %+v\n", nodesData))
	return followerPods, nil
}

func (r *RedisClusterReconciler) makeLeaderPod(redisCluster *dbv1.RedisCluster, leaderName string) (corev1.Pod, error) {
	role := "leader"
	preferredLabelSelectorRequirement := []metav1.LabelSelectorRequirement{{Key: "redis-node-role", Operator: metav1.LabelSelectorOpIn, Values: []string{"leader"}}}
	pod := r.makeRedisPod(redisCluster, role, leaderName, leaderName, preferredLabelSelectorRequirement, true)

	if err := ctrl.SetControllerReference(redisCluster, &pod, r.Scheme); err != nil {
		return pod, err
	}
	return pod, nil
}

// Creates one or more leader pods; waits for available IP before returning
func (r *RedisClusterReconciler) CreateRedisLeaderPods(redisCluster *dbv1.RedisCluster, leaderNames []string) ([]corev1.Pod, error) {
	var e error
	var leaderPods []corev1.Pod
	if len(leaderNames) > 0 {
		for _, leaderName := range leaderNames {
			pod, e := r.makeLeaderPod(redisCluster, leaderName)
			if e != nil {
				return nil, errors.Errorf("Create redis leader pods error: Could not create pod %+v. %+v", leaderName, e.Error())
			}
			leaderPods = append(leaderPods, pod)
		}

		applyOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}

		for i, pod := range leaderPods {
			e := r.Create(context.Background(), &leaderPods[i], applyOpts...)
			if e != nil && !apierrors.IsAlreadyExists(e) && !apierrors.IsConflict(e) {
				return nil, errors.Errorf("Create redis leader pods error: Could not validate pod creation %+v. %+v", pod.Name, e.Error())
			}
		}

		leaderPods, e = r.waitForPodNetworkInterface(leaderPods...)
		if e != nil {
			return nil, errors.Errorf("Create redis leader pods error: Could not wait for pod creation %+v. %+v", leaderPods, e.Error())
		}

		r.Log.Info(fmt.Sprintf("New leader pods created: %v ", leaderNames))
	}
	return leaderPods, e
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
					Port:       int32(r.RedisCLI.PortAsInt),
					TargetPort: intstr.FromInt(r.RedisCLI.PortAsInt),
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

func (r *RedisClusterReconciler) getPodsByLabel(key string, val string) ([]corev1.Pod, error) {
	var pods corev1.PodList
	e := r.List(context.TODO(), &pods, client.MatchingLabels{key: val})
	return pods.Items, e
}

func (r *RedisClusterReconciler) getRedisPodsView() (*RedisPodsView, error) {
	v := &RedisPodsView{
		PodViewByIp:    make(map[string]*RedisPodView),
		PodNameToPodIp: make(map[string]string),
	}
	leaderPods, e := r.getPodsByLabel("redis-node-role", "leader")
	if e != nil {
		return nil, e
	}
	println("Got leaders")
	followerPods, e := r.getPodsByLabel("redis-node-role", "follower")
	if e != nil {
		return nil, e
	}
	println("Got followers")
	viewByName := make(map[string]*RedisPodView)
	for _, p := range leaderPods {
		viewByName[p.GetName()] = &RedisPodView{
			Name:            p.GetName(),
			Namespace:       p.GetNamespace(),
			IP:              p.Status.PodIP,
			Phase:           string(p.Status.Phase),
			LeaderName:      p.GetName(),
			IsLeader:        true,
			FollowersByName: make([]string, 0),
			Labels:          p.GetLabels(),
			Pod:             p,
		}
	}
	println("Filled leaders")
	for _, p := range followerPods {
		viewByName[p.GetName()] = &RedisPodView{
			Name:            p.GetName(),
			Namespace:       p.GetNamespace(),
			IP:              p.Status.PodIP,
			Phase:           string(p.Status.Phase),
			LeaderName:      p.GetLabels()["leader-name"],
			IsLeader:        false,
			FollowersByName: make([]string, 0),
			Labels:          p.GetLabels(),
			Pod:             p,
		}
		leaderName := viewByName[p.GetName()].LeaderName
		viewByName[leaderName].FollowersByName = append(viewByName[leaderName].FollowersByName, p.Name)
	}
	println("Filled followers")
	for name, p := range viewByName {
		v.PodViewByIp[p.IP] = p
		v.PodNameToPodIp[name] = p.IP
	}
	println("Linked followers to leaders lists")
	return v, nil
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

// todo: requires exponential backoff
func (r *RedisClusterReconciler) waitForPodReady(pods ...corev1.Pod) ([]corev1.Pod, error) {
	var readyPods []corev1.Pod
	for _, pod := range pods {
		key, err := client.ObjectKeyFromObject(&pod)
		if err != nil {
			return nil, err
		}
		r.Log.Info(fmt.Sprintf("Waiting for pod ready: %s(%s)", pod.Name, pod.Status.PodIP))
		if pollErr := wait.PollImmediate(3*r.Config.Times.PodReadyCheckInterval, 5*r.Config.Times.PodReadyCheckTimeout, func() (bool, error) {
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

// Method used to wait for one or more pods to have an IP address
func (r *RedisClusterReconciler) waitForPodNetworkInterface(pods ...corev1.Pod) ([]corev1.Pod, error) {
	r.Log.Info(fmt.Sprintf("Waiting for pod network interfaces..."))
	var readyPods []corev1.Pod
	for _, pod := range pods {
		key, err := client.ObjectKeyFromObject(&pod)
		if pollErr := wait.PollImmediate(3*r.Config.Times.PodNetworkCheckInterval, 5*r.Config.Times.PodNetworkCheckTimeout, func() (bool, error) {
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

// TODO should wait as long as delete grace period
func (r *RedisClusterReconciler) waitForPodDelete(pods ...corev1.Pod) error {
	for _, p := range pods {
		key, err := client.ObjectKeyFromObject(&p)
		if err != nil {
			return err
		}
		r.Log.Info(fmt.Sprintf("Waiting for pod delete: %s", p.Name))
		if pollErr := wait.Poll(3*r.Config.Times.PodDeleteCheckInterval, 5*r.Config.Times.PodDeleteCheckTimeout, func() (bool, error) {
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

type K8sManager struct {
	client.Client
	Log    logr.Logger
	Config *RedisOperatorConfig
	Scheme *runtime.Scheme
}

func (r *K8sManager) WritePodAnnotations(annotations map[string]string, pods ...corev1.Pod) error {
	annotationsString := ""
	for key, val := range annotations {
		annotationsString = fmt.Sprintf("\"%s\": \"%s\",%s", key, val, annotationsString)
	}
	patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%s}}}`, annotationsString[:len(annotationsString)-1]))
	for i, pod := range pods {
		if err := r.Patch(context.Background(), &pods[i], client.RawPatch(types.StrategicMergePatchType, patch)); err != nil {
			r.Log.Error(err, fmt.Sprintf("Failed to patch the annotations on pod %s (%s)", pod.Name, pod.Status.PodIP))
		}
	}
	return nil
}
