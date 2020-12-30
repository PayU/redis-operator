package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"

	dbv1 "github.com/PayU/Redis-Operator/api/v1"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *RedisClusterReconciler) getRedisClusterPods(redisCluster *dbv1.RedisCluster, podType ...string) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	matchingLabels := make(map[string]string)
	matchingLabels["app"] = redisCluster.Spec.PodLabelSelector.App

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

	sortedPods := pods.Items
	sort.Slice(sortedPods, func(i, j int) bool {
		return pods.Items[i].Labels["node-number"] < pods.Items[j].Labels["node-number"]
	})

	return sortedPods, nil
}

func (r *RedisClusterReconciler) getPodByIP(podIP string) (corev1.Pod, error) {
	var podList corev1.PodList
	err := r.List(context.Background(), &podList, client.MatchingFields{"status.podIP": podIP})
	if err != nil {
		return corev1.Pod{}, err
	}
	if len(podList.Items) == 0 {
		return corev1.Pod{}, errors.Errorf("No pod found for IP [%s]", podIP)
	}
	return podList.Items[0], nil
}

func makeRedisPod(redisCluster *dbv1.RedisCluster, nodeRole string, leaderNumber string, nodeNumber string, preferredLabelSelectorRequirement []metav1.LabelSelectorRequirement) corev1.Pod {
	podLabels := make(map[string]string)
	containers := make([]corev1.Container, 0)
	initContainers := make([]corev1.Container, 0)
	redisContainerEnvVariables := []corev1.EnvVar{
		{Name: "PORT", Value: "6379"},
	}

	podLabels["app"] = redisCluster.Spec.PodLabelSelector.App
	podLabels["redis-node-role"] = nodeRole
	podLabels["leader-number"] = leaderNumber
	podLabels["node-number"] = nodeNumber

	for _, envStruct := range redisCluster.Spec.RedisContainerEnvVariables {
		if envStruct.Name != "PORT" {
			redisContainerEnvVariables = append(redisContainerEnvVariables, corev1.EnvVar{
				Name:  envStruct.Name,
				Value: envStruct.Value,
			})
		}
	}

	imagePullSecrets := []corev1.LocalObjectReference{
		{Name: redisCluster.Spec.ImagePullSecrets},
	}

	containers = []corev1.Container{
		{
			Name:            "redis-container",
			Image:           redisCluster.Spec.Image,
			ImagePullPolicy: redisCluster.Spec.ImagePullPolicy,
			Resources:       corev1.ResourceRequirements(redisCluster.Spec.RedisContainerResources),
			Env:             redisContainerEnvVariables,
			Ports: []corev1.ContainerPort{
				{ContainerPort: 6379, Name: "redis", Protocol: "TCP"},
			},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "redis-node-configuration", MountPath: "/usr/local/etc/redis"},
			},
		},
	}

	if redisCluster.Spec.PrometheusExporter != (dbv1.PrometheusExporterOpt{}) {
		containers = append(containers, corev1.Container{
			Name:            "redis-metric-exporter",
			Image:           redisCluster.Spec.PrometheusExporter.Image,
			ImagePullPolicy: redisCluster.Spec.PrometheusExporter.ImagePullPolicy,
			Ports: []corev1.ContainerPort{
				{ContainerPort: redisCluster.Spec.PrometheusExporter.Port, Name: "client", Protocol: "TCP"},
			},
		})
	}

	volumes := []corev1.Volume{
		{
			Name: "redis-node-configuration",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: "redis-node-settings-config-map",
					},
				},
			},
		},
		{
			Name: "host-sys",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/sys",
				},
			},
		},
	}

	var affinity *corev1.Affinity = nil
	if redisCluster.Spec.Affinity != (dbv1.TopologyKeys{}) {
		affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{}}
		if redisCluster.Spec.Affinity.HostTopologyKey != "" {
			affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{Key: "app", Operator: metav1.LabelSelectorOpIn, Values: []string{redisCluster.Spec.PodLabelSelector.App}},
						},
					},
					TopologyKey: redisCluster.Spec.Affinity.HostTopologyKey,
				},
			}
		}

		if redisCluster.Spec.Affinity.ZoneTopologyKey != "" {
			affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: preferredLabelSelectorRequirement,
						},
						TopologyKey: redisCluster.Spec.Affinity.ZoneTopologyKey,
					},
				},
			}
		}
	}

	if redisCluster.Spec.InitContainer.Image != "" {
		initContainerRunAsNonRoot := false
		var runAsUser int64 = 0
		privileged := true

		/*
		* init container will make sure to avoid redis kernel warning: '# WARNING: The TCP backlog setting of 511 cannot be enforced because /proc/sys/net/core/somaxconn is set to the lower value of 128'
		* 1) sysctl -w net.core.somaxconn=1024 -> set the maximum number of "backlogged sockets". the backlog parameter specifies the number of pending connections the connection queue will hold. Default is 128.
		* 2) echo never > /host-sys/kernel/mm/transparent_hugepage/enabled -> disabling the kernel setting Transparent Huge Pages (THP) - recommended for redis (http://doc.nuodb.com/Latest/Content/Note-About-%20Using-Transparent-Huge-Pages.htm)
		* 3) grep -q -F [never] /sys/kernel/mm/transparent_hugepage/enabled -> Checking if transparent_hugepage settings applied
		 */

		command := "install_packages systemd procps && sysctl -w net.core.somaxconn=1024"

		if redisCluster.Spec.InitContainer.EnabledHugepage {
			command += " && echo never > /host-sys/kernel/mm/transparent_hugepage/enabled && grep -q -F [never] /sys/kernel/mm/transparent_hugepage/enabled"
		}

		initContainers = []corev1.Container{
			{
				Name:            "redis-init-container",
				Image:           redisCluster.Spec.InitContainer.Image,
				ImagePullPolicy: redisCluster.Spec.InitContainer.ImagePullPolicy,
				SecurityContext: &corev1.SecurityContext{
					RunAsNonRoot: &initContainerRunAsNonRoot,
					Privileged:   &privileged,
					RunAsUser:    &runAsUser,
				},
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      "host-sys",
						MountPath: "/host-sys",
					},
				},
				Command: []string{
					"/bin/sh",
					"-c",
					command,
				},
			},
		}
	}

	pod := corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("redis-node-%s", nodeNumber),
			Namespace:   redisCluster.ObjectMeta.Namespace,
			Labels:      podLabels,
			Annotations: redisCluster.Spec.PodAnnotations,
		},
		Spec: corev1.PodSpec{
			ImagePullSecrets: imagePullSecrets,
			InitContainers:   initContainers,
			Containers:       containers,
			Volumes:          volumes,
			Affinity:         affinity,
		},
	}

	return pod
}

func (r *RedisClusterReconciler) makeFollowerPod(redisCluster *dbv1.RedisCluster, nodeNumber string, leaderNumber string) (corev1.Pod, error) {
	preferredLabelSelectorRequirement := []metav1.LabelSelectorRequirement{{Key: "leader-number", Operator: metav1.LabelSelectorOpIn, Values: []string{leaderNumber}}}
	pod := makeRedisPod(redisCluster, "follower", leaderNumber, nodeNumber, preferredLabelSelectorRequirement)

	if err := ctrl.SetControllerReference(redisCluster, &pod, r.Scheme); err != nil {
		return pod, err
	}

	return pod, nil
}

func (r *RedisClusterReconciler) createRedisFollowerPods(redisCluster *dbv1.RedisCluster, nodeNumbers ...NodeNumbers) ([]corev1.Pod, error) {
	if len(nodeNumbers) == 0 {
		return nil, errors.Errorf("Failed to create Redis followers - no node numbers")
	}

	var followerPods []corev1.Pod
	createOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}

	for _, nodeNumber := range nodeNumbers {
		pod, err := r.makeFollowerPod(redisCluster, nodeNumber[0], nodeNumber[1])
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

	r.Log.Info(fmt.Sprintf("New follower pods created: %v", nodeNumbers))
	return followerPods, nil
}

func (r *RedisClusterReconciler) makeLeaderPod(redisCluster *dbv1.RedisCluster, nodeNumber string) (corev1.Pod, error) {
	preferredLabelSelectorRequirement := []metav1.LabelSelectorRequirement{{Key: "redis-node-role", Operator: metav1.LabelSelectorOpIn, Values: []string{"leader"}}}
	pod := makeRedisPod(redisCluster, "leader", nodeNumber, nodeNumber, preferredLabelSelectorRequirement)

	if err := ctrl.SetControllerReference(redisCluster, &pod, r.Scheme); err != nil {
		return pod, err
	}
	return pod, nil
}

// Creates one or more leader pods; waits for available IP before returing
func (r *RedisClusterReconciler) createRedisLeaderPods(redisCluster *dbv1.RedisCluster, nodeNumbers ...string) ([]corev1.Pod, error) {

	if len(nodeNumbers) == 0 {
		return nil, errors.New("Failed to create leader pods - no node numbers")
	}

	var leaderPods []corev1.Pod
	for _, nodeNumber := range nodeNumbers {
		pod, err := r.makeLeaderPod(redisCluster, nodeNumber)
		if err != nil {
			return nil, err
		}
		leaderPods = append(leaderPods, pod)
	}

	applyOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}
	for _, pod := range leaderPods {
		err := r.Create(context.Background(), &pod, applyOpts...)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
	}

	leaderPods, err := r.waitForPodNetworkInterface(leaderPods...)
	if err != nil {
		return nil, err
	}

	r.Log.Info(fmt.Sprintf("New leader pods ready: %v ", nodeNumbers))
	return leaderPods, nil
}

// Method used to wait for one or more pods to have an IP address
func (r *RedisClusterReconciler) waitForPodNetworkInterface(pods ...corev1.Pod) ([]corev1.Pod, error) {
	r.Log.Info(fmt.Sprintf("Waiting for pod network interfaces..."))
	var resultPods []corev1.Pod
	for _, pod := range pods {
		key, err := client.ObjectKeyFromObject(&pod)
		if pollErr := wait.PollImmediate(genericCheckInterval, genericCheckTimeout, func() (bool, error) {
			if err = r.Get(context.Background(), key, &pod); err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				return false, err
			}
			if pod.Status.PodIP == "" {
				return false, nil
			}
			resultPods = append(resultPods, pod)
			return true, nil
		}); pollErr != nil {
			return nil, pollErr
		}
	}
	return resultPods, nil
}

func (r *RedisClusterReconciler) waitForPodDelete(pods ...corev1.Pod) error {
	for _, p := range pods {
		key, err := client.ObjectKeyFromObject(&p)
		if err != nil {
			return err
		}
		if pollErr := wait.Poll(genericCheckInterval, genericCheckTimeout, func() (bool, error) {
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

func (r *RedisClusterReconciler) makeService(redisCluster *dbv1.RedisCluster) (corev1.Service, error) {
	serviceSelector := make(map[string]string)
	serviceSelector["app"] = redisCluster.Spec.PodLabelSelector.App

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
			Selector: serviceSelector,
		},
	}

	if err := ctrl.SetControllerReference(redisCluster, &service, r.Scheme); err != nil {
		return service, err
	}

	return service, nil
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

func (r *RedisClusterReconciler) makeHeadlessService(redisCluster *dbv1.RedisCluster) (corev1.Service, error) {
	serviceSelector := make(map[string]string)
	serviceSelector["app"] = redisCluster.Spec.PodLabelSelector.App

	headlessService := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-cluster-headless-service",
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
			Selector:  serviceSelector,
			ClusterIP: "None",
		},
	}

	if err := ctrl.SetControllerReference(redisCluster, &headlessService, r.Scheme); err != nil {
		return headlessService, err
	}

	return headlessService, nil
}

func (r *RedisClusterReconciler) createRedisHeadlessService(redisCluster *dbv1.RedisCluster) (*corev1.Service, error) {
	svc, err := r.makeHeadlessService(redisCluster)
	if err != nil {
		return nil, err
	}
	err = r.Create(context.Background(), &svc)
	if !apierrors.IsAlreadyExists(err) {
		return nil, err
	}

	return &svc, nil
}

func (r *RedisClusterReconciler) makeRedisSettingsConfigMap(redisCluster *dbv1.RedisCluster) (corev1.ConfigMap, error) {
	data := make(map[string]string)
	data["redis.conf"] = redisConf

	redisSettingConfigMap := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-node-settings-config-map",
			Namespace: redisCluster.ObjectMeta.Namespace,
		},
		Data: data,
	}

	if err := ctrl.SetControllerReference(redisCluster, &redisSettingConfigMap, r.Scheme); err != nil {
		return redisSettingConfigMap, err
	}

	return redisSettingConfigMap, nil
}

func (r *RedisClusterReconciler) createRedisSettingConfigMap(redisCluster *dbv1.RedisCluster) (*corev1.ConfigMap, error) {
	cm, err := r.makeRedisSettingsConfigMap(redisCluster)
	if err != nil {
		return nil, err
	}
	err = r.Create(context.Background(), &cm)
	if !apierrors.IsAlreadyExists(err) {
		return nil, err
	}
	return &cm, nil
}

func (r *RedisClusterReconciler) waitForPodReady(pod *corev1.Pod) error {
	key, err := client.ObjectKeyFromObject(pod)
	if err != nil {
		return err
	}
	r.Log.Info(fmt.Sprintf("Waiting for pod ready: %s(%s)", pod.Name, pod.Status.PodIP))
	return wait.PollImmediate(genericCheckInterval, genericCheckTimeout, func() (bool, error) {
		err := r.Get(context.Background(), key, pod)
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
		return true, nil
	})
}
