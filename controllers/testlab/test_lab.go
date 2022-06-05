package testlab

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	dbv1 "github.com/PayU/redis-operator/api/v1"
	"github.com/PayU/redis-operator/controllers/rediscli"
	"github.com/PayU/redis-operator/controllers/redisclient"
	"github.com/PayU/redis-operator/controllers/view"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type TestLab struct {
	Client             client.Client
	RedisCLI           *rediscli.RedisCLI
	Cluster            *dbv1.RedisCluster
	RedisClusterClient *redisclient.RedisClusterClient
	Log                logr.Logger
	Report             string
}

var fetchViewInterval = 10 * time.Second
var fetchViewTimeOut = 2 * time.Minute

var clusterHealthCheckInterval = 20 * time.Second
var clusterHealthCheckTimeOutLimit = 3 * time.Minute

var randomChoiceRetries int = 4

var sleepPerTest time.Duration = 10 * time.Second
var sleepPerPodCheck time.Duration = 2 * time.Second
var sleepPerHealthCheck time.Duration = 10 * time.Second

var dataWriteRetries int = 5
var dataReadRetries int = 5

var intervalsBetweenWrites time.Duration = 2 * time.Second

var totalDataWrites int = 20

func (t *TestLab) RunTest(wg *sync.WaitGroup, nodes *map[string]*view.NodeStateView) {
	defer wg.Done()
	t.Report = "\n[TEST LAB] Cluster test report:\n"
	isReady := t.waitForHealthyCluster(nodes)
	if !isReady {
		return
	}
	t.Report += "\n"
	test_1 := t.test_delete_follower_with_data(nodes, 1)
	if !test_1 {
		return
	}
	test_2 := t.test_delete_leader_with_data(nodes, 2)
	if !test_2 {
		return
	}
	test_3 := t.test_delete_leader_and_follower_with_data(nodes, 3)
	if !test_3 {
		return
	}
	test_4 := t.test_delete_all_followers_with_data(nodes, 4)
	if !test_4 {
		return
	}
	test_5 := t.test_delete_all_azs_beside_one_with_data(nodes, 5)
	if !test_5 {
		return
	}
	test_6 := t.test_delete_leader_and_all_its_followers_with_data(nodes, 6)
	if !test_6 {
		return
	}
}

func (t *TestLab) testDataWrites(total int) (successfulWrites int, data map[string]string) {
	data = map[string]string{}
	successfulWrites = 0
	for i := 0; i < total; i++ {
		key := "key" + fmt.Sprintf("%v", i)
		val := "val" + fmt.Sprintf("%v", i)
		err := t.RedisClusterClient.Set(key, val, dataWriteRetries)
		if err == nil {
			successfulWrites++
			data[key] = val
		}
		time.Sleep(intervalsBetweenWrites)
	}
	return successfulWrites, data
}

func (t *TestLab) testDataReads(data map[string]string) (successfulReads int) {
	successfulReads = 0
	for k, expected_v := range data {
		actual_v, err := t.RedisClusterClient.Get(k, dataReadRetries)
		if err == nil {
			if expected_v == actual_v {
				successfulReads++
			}
		}
	}
	return successfulReads
}

func (t *TestLab) analyzeDataResults(total int, successfulWrites int, successfulReads int) {
	writeSuccessRate := (successfulWrites * 100.0) / (total)
	readSuccessRate := 0
	if successfulWrites > 0 {
		readSuccessRate = (successfulReads * 100.0) / (successfulWrites)
	}
	t.Report += fmt.Sprintf("[TEST LAB] Total               : [%v]\n", total)
	t.Report += fmt.Sprintf("[TEST LAB] Successful writes   : [%v]\n", successfulWrites)
	t.Report += fmt.Sprintf("[TEST LAB] Successful reads    : [%v]\n", successfulReads)
	t.Report += fmt.Sprintf("[TEST LAB] Writes success rate : [%v%v]\n", writeSuccessRate, "%")
	t.Report += fmt.Sprintf("[TEST LAB] Read success rate   : [%v%v]\n", readSuccessRate, "%")
}

func (t *TestLab) test_delete_follower_with_data(nodes *map[string]*view.NodeStateView, testNum int) bool {
	if t.RedisClusterClient == nil {
		return false
	}
	t.RedisClusterClient.FlushAllData()
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	result := false
	sw := 0
	sr := 0
	data := map[string]string{}
	wg.Add(2)
	go func(wg *sync.WaitGroup) {
		mutex.Lock()
		defer wg.Done()
		mutex.Unlock()
		result = t.test_delete_follower(nodes, testNum)
	}(&wg)
	go func(wg *sync.WaitGroup) {
		mutex.Lock()
		defer wg.Done()
		mutex.Unlock()
		sw, data = t.testDataWrites(totalDataWrites)
	}(&wg)
	wg.Wait()
	sr = t.testDataReads(data)
	t.analyzeDataResults(totalDataWrites, sw, sr)
	return result
}

func (t *TestLab) test_delete_follower(nodes *map[string]*view.NodeStateView, testNum int) bool {
	time.Sleep(sleepPerTest)
	t.Log.Info("[TEST LAB] Running test: Delete follower...")
	t.Report += fmt.Sprintf("\n[TEST LAB] Test %v delete follower...", testNum)
	randomFollower := t.PickRandomeFollower(map[string]bool{}, nodes, randomChoiceRetries)
	result := t.test_delete_pods([]string{randomFollower}, nodes)
	t.Report += fmt.Sprintf("\n[TEST LAB] Test %v: delete follower result [%v]\n", testNum, result)
	return result
}

func (t *TestLab) test_delete_leader_with_data(nodes *map[string]*view.NodeStateView, testNum int) bool {
	if t.RedisClusterClient == nil {
		return false
	}
	t.RedisClusterClient.FlushAllData()
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	result := false
	sw := 0
	sr := 0
	data := map[string]string{}
	wg.Add(2)
	go func(wg *sync.WaitGroup) {
		mutex.Lock()
		defer wg.Done()
		mutex.Unlock()
		result = t.test_delete_leader(nodes, testNum)
	}(&wg)
	go func(wg *sync.WaitGroup) {
		mutex.Lock()
		defer wg.Done()
		mutex.Unlock()
		sw, data = t.testDataWrites(totalDataWrites)
	}(&wg)
	wg.Wait()
	sr = t.testDataReads(data)
	t.analyzeDataResults(totalDataWrites, sw, sr)
	return result
}

func (t *TestLab) test_delete_leader(nodes *map[string]*view.NodeStateView, testNum int) bool {
	time.Sleep(sleepPerTest)
	t.Log.Info("[TEST LAB] Running test: Delete leader...")
	t.Report += fmt.Sprintf("\n[TEST LAB] Test %v delete leader...", testNum)
	randomLeader := t.PickRandomeLeader(map[string]bool{}, nodes, randomChoiceRetries)
	result := t.test_delete_pods([]string{randomLeader}, nodes)
	t.Report += fmt.Sprintf("\n[TEST LAB] Test %v: delete leader result [%v]\n", testNum, result)
	return result
}

func (t *TestLab) test_delete_leader_and_follower_with_data(nodes *map[string]*view.NodeStateView, testNum int) bool {
	if t.RedisClusterClient == nil {
		return false
	}
	t.RedisClusterClient.FlushAllData()
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	result := false
	sw := 0
	sr := 0
	data := map[string]string{}
	wg.Add(2)
	go func(wg *sync.WaitGroup) {
		mutex.Lock()
		defer wg.Done()
		mutex.Unlock()
		result = t.test_delete_leader_and_follower(nodes, testNum)
	}(&wg)
	go func(wg *sync.WaitGroup) {
		mutex.Lock()
		defer wg.Done()
		mutex.Unlock()
		sw, data = t.testDataWrites(totalDataWrites)
	}(&wg)
	wg.Wait()
	sr = t.testDataReads(data)
	t.analyzeDataResults(totalDataWrites, sw, sr)
	return result
}

func (t *TestLab) test_delete_leader_and_follower(nodes *map[string]*view.NodeStateView, testNum int) bool {
	time.Sleep(sleepPerTest)
	t.Log.Info("[TEST LAB] Running test: Delete leader and follower...")
	t.Report += fmt.Sprintf("\n[TEST LAB] Test %v delete leader and follower...", testNum)
	randomFollower := t.PickRandomeFollower(map[string]bool{}, nodes, randomChoiceRetries)
	f, exists := (*nodes)[randomFollower]
	if !exists {
		return false
	}
	randomLeader := t.PickRandomeLeader(map[string]bool{f.LeaderName: true}, nodes, randomChoiceRetries)
	result := t.test_delete_pods([]string{randomFollower, randomLeader}, nodes)
	t.Report += fmt.Sprintf("\n[TEST LAB] Test %v: delete leader and follower result [%v]\n", testNum, result)
	return result
}

func (t *TestLab) test_delete_all_followers_with_data(nodes *map[string]*view.NodeStateView, testNum int) bool {
	if t.RedisClusterClient == nil {
		return false
	}
	t.RedisClusterClient.FlushAllData()
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	result := false
	sw := 0
	sr := 0
	data := map[string]string{}
	wg.Add(2)
	go func(wg *sync.WaitGroup) {
		mutex.Lock()
		defer wg.Done()
		mutex.Unlock()
		result = t.test_delete_all_followers(nodes, testNum)
	}(&wg)
	go func(wg *sync.WaitGroup) {
		mutex.Lock()
		defer wg.Done()
		mutex.Unlock()
		sw, data = t.testDataWrites(totalDataWrites)
	}(&wg)
	wg.Wait()
	sr = t.testDataReads(data)
	t.analyzeDataResults(totalDataWrites, sw, sr)
	return result
}

func (t *TestLab) test_delete_all_followers(nodes *map[string]*view.NodeStateView, testNum int) bool {
	time.Sleep(sleepPerTest)
	t.Log.Info("[TEST LAB] Running test: Delete all followers...")
	t.Report += fmt.Sprintf("\n[TEST LAB] Test %v delete all followers...", testNum)
	followers := []string{}
	for _, n := range *nodes {
		if n.Name != n.LeaderName {
			followers = append(followers, n.Name)
		}
	}
	result := t.test_delete_pods(followers, nodes)
	t.Report += fmt.Sprintf("\n[TEST LAB] Test %v: delete all followers result [%v]\n", testNum, result)
	return result
}

func (t *TestLab) test_delete_leader_and_all_its_followers_with_data(nodes *map[string]*view.NodeStateView, testNum int) bool {
	if t.RedisClusterClient == nil {
		return false
	}
	t.RedisClusterClient.FlushAllData()
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	result := false
	sw := 0
	sr := 0
	data := map[string]string{}
	wg.Add(2)
	go func(wg *sync.WaitGroup) {
		mutex.Lock()
		defer wg.Done()
		mutex.Unlock()
		result = t.test_delete_leader_and_all_its_followers(nodes, testNum)
	}(&wg)
	go func(wg *sync.WaitGroup) {
		mutex.Lock()
		defer wg.Done()
		mutex.Unlock()
		sw, data = t.testDataWrites(totalDataWrites)
	}(&wg)
	wg.Wait()
	sr = t.testDataReads(data)
	t.analyzeDataResults(totalDataWrites, sw, sr)
	return result
}

func (t *TestLab) test_delete_leader_and_all_its_followers(nodes *map[string]*view.NodeStateView, testNum int) bool {
	time.Sleep(sleepPerTest)
	t.Log.Info("[TEST LAB] Running test: Delete leader and all its folowers...")
	t.Report += fmt.Sprintf("\n[TEST LAB] Test %v delete leader and all his followers...", testNum)
	toDelete := []string{}
	randomFollower := t.PickRandomeFollower(map[string]bool{}, nodes, randomChoiceRetries)
	f, exists := (*nodes)[randomFollower]
	if !exists {
		return false
	}
	for _, n := range *nodes {
		if n.LeaderName == f.LeaderName {
			toDelete = append(toDelete, n.Name)
		}
	}
	result := t.test_delete_pods(toDelete, nodes)
	t.Report += fmt.Sprintf("\n[TEST LAB] Test %v: delete leader and all his followers result [%v]\n", testNum, result)
	return result
}

func (t *TestLab) test_delete_all_azs_beside_one_with_data(nodes *map[string]*view.NodeStateView, testNum int) bool {
	if t.RedisClusterClient == nil {
		return false
	}
	t.RedisClusterClient.FlushAllData()
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	result := false
	sw := 0
	sr := 0
	data := map[string]string{}
	wg.Add(2)
	go func(wg *sync.WaitGroup) {
		mutex.Lock()
		defer wg.Done()
		mutex.Unlock()
		result = t.test_delete_all_azs_beside_one(nodes, testNum)
	}(&wg)
	go func(wg *sync.WaitGroup) {
		mutex.Lock()
		defer wg.Done()
		mutex.Unlock()
		sw, data = t.testDataWrites(totalDataWrites)
	}(&wg)
	wg.Wait()
	sr = t.testDataReads(data)
	t.analyzeDataResults(totalDataWrites, sw, sr)
	return result
}

func (t *TestLab) test_delete_all_azs_beside_one(nodes *map[string]*view.NodeStateView, testNum int) bool {
	time.Sleep(sleepPerTest)
	t.Log.Info("[TEST LAB] Running test: Simulating loss of all az's beside one...")
	t.Report += fmt.Sprintf("\n[TEST LAB] Test %v delete all pods beside one replica foreach set...", testNum)
	del := map[string]bool{}
	keep := map[string]bool{}
	d := false
	for _, n := range *nodes {
		if n.Name == n.LeaderName {
			del[n.Name] = d
			d = !d
		}
	}
	for _, n := range *nodes {
		if n.Name != n.LeaderName {
			delLeader, leaderInMap := del[n.LeaderName]
			if !leaderInMap {
				del[n.Name] = false
				keep[n.LeaderName] = true
			} else {
				if delLeader {
					_, hasReplicaToKeep := keep[n.LeaderName]
					if hasReplicaToKeep {
						del[n.Name] = true
					} else {
						del[n.Name] = false
						keep[n.LeaderName] = true
					}
				} else {
					del[n.Name] = true
				}
			}
		}
	}
	toDelete := []string{}
	for n, d := range del {
		if d {
			toDelete = append(toDelete, n)
		}
	}
	result := t.test_delete_pods(toDelete, nodes)
	t.Report += fmt.Sprintf("\n[TEST LAB] Test %v: delete all pods beside one replica foreach set result [%v]\n", testNum, result)
	return result
}

func (t *TestLab) PickRandomeFollower(exclude map[string]bool, nodes *map[string]*view.NodeStateView, retry int) string {
	k := rand.Intn(t.Cluster.Spec.LeaderFollowersCount*t.Cluster.Spec.LeaderCount - len(exclude))
	i := 0
	for _, n := range *nodes {
		if _, ex := exclude[n.Name]; ex || n.Name == n.LeaderName {
			continue
		}
		if i == k {
			return n.Name
		}
		i++
	}
	if retry == 0 {
		return ""
	}
	return t.PickRandomeFollower(exclude, nodes, retry-1)
}

func (t *TestLab) PickRandomeLeader(exclude map[string]bool, nodes *map[string]*view.NodeStateView, retry int) string {
	k := rand.Intn(t.Cluster.Spec.LeaderCount - len(exclude))
	i := 0
	for _, n := range *nodes {
		if _, ex := exclude[n.Name]; ex || n.Name != n.LeaderName {
			continue
		}
		if i == k {
			return n.Name
		}
		i++
	}
	if retry == 0 {
		return ""
	}
	return t.PickRandomeLeader(exclude, nodes, retry-1)
}

func (t *TestLab) checkIfMaster(nodeIP string) (bool, error) {
	info, _, err := t.RedisCLI.Info(nodeIP)
	if err != nil || info == nil {
		return false, err
	}
	if info.Replication["role"] == "master" {
		return true, nil
	}
	return false, nil
}

func (t *TestLab) test_delete_pods(podsToDelete []string, nodes *map[string]*view.NodeStateView) bool {
	v := t.WaitForClusterView()
	if v == nil {
		return false
	}
	for _, toDelete := range podsToDelete {
		node, exists := v.Nodes[toDelete]
		if !exists || node == nil {
			continue
		}
		time.Sleep(1 * time.Second)
		t.deletePod(node.Pod)
	}
	return t.waitForHealthyCluster(nodes)
}

func (t *TestLab) WaitForClusterView() *view.RedisClusterView {
	var v *view.RedisClusterView = nil
	var ok bool = false
	if pollErr := wait.PollImmediate(fetchViewInterval, fetchViewTimeOut, func() (bool, error) {
		v, ok = t.newRedisClusterView()
		if !ok {
			return false, nil
		}
		return true, nil
	}); pollErr != nil {
		t.Report += fmt.Sprintf("\n[TEST LAB] Error: Could not fetch cluster view, prob intervals: [%v], probe timeout: [%v]\n", fetchViewInterval, fetchViewTimeOut)
	}
	return v
}

func (t *TestLab) waitForHealthyCluster(nodes *map[string]*view.NodeStateView) bool {
	time.Sleep(sleepPerHealthCheck)
	t.Report += fmt.Sprintf("\n[TEST LAB] Waiting for cluster to be declared ready...")
	isHealthyCluster := false
	if pollErr := wait.PollImmediate(clusterHealthCheckInterval, clusterHealthCheckTimeOutLimit, func() (bool, error) {
		if t.isClusterAligned(nodes) {
			isHealthyCluster = true
			return true, nil
		}
		return false, nil
	}); pollErr != nil {
		t.Report += fmt.Sprintf("\n[TEST LAB] Error while waiting for cluster to heal, probe intervals: [%v], probe timeout: [%v]\n", clusterHealthCheckInterval, clusterHealthCheckTimeOutLimit)
		return false
	}
	return isHealthyCluster
}

func (t *TestLab) isClusterAligned(nodes *map[string]*view.NodeStateView) bool {
	v := t.WaitForClusterView()
	if v == nil {
		return false
	}
	t.Log.Info("[TEST LAB] Checking if cluster is ready...")
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	aligned := true
	for _, n := range *nodes {
		wg.Add(1)
		go func(n *view.NodeStateView, wg *sync.WaitGroup) {
			time.Sleep(sleepPerPodCheck)
			mutex.Lock()
			defer wg.Done()
			mutex.Unlock()
			if !aligned {
				return
			}
			node, exists := v.Nodes[n.Name]
			if !exists || node == nil {
				mutex.Lock()
				aligned = false
				mutex.Unlock()
				return
			}
			if node.IsLeader {
				if !t.isLeaderAligned(node) {
					mutex.Lock()
					aligned = false
					mutex.Unlock()
					return
				}
			} else {
				if !t.isFollowerAligned(node, nodes) {
					mutex.Lock()
					aligned = false
					mutex.Unlock()
					return
				}
			}
		}(n, &wg)
	}
	wg.Wait()
	totalExpectedNodes := t.Cluster.Spec.LeaderCount * (t.Cluster.Spec.LeaderFollowersCount + 1)
	clusterOK := aligned && len(v.Nodes) == totalExpectedNodes && len(*nodes) == totalExpectedNodes
	if clusterOK {
		t.RedisClusterClient = redisclient.GetRedisClusterClient(v, t.RedisCLI)
	}
	return clusterOK
}

func (t *TestLab) isFollowerAligned(n *view.NodeView, nodes *map[string]*view.NodeStateView) bool {
	_, leaderExists := (*nodes)[n.LeaderName]
	if !leaderExists {
		return false
	}
	isMaster, e := t.checkIfMaster(n.Ip)
	if e != nil || isMaster {
		return false
	}
	return true
}

func (t *TestLab) isLeaderAligned(n *view.NodeView) bool {
	isMaster, e := t.checkIfMaster(n.Ip)
	if e != nil || !isMaster {
		return false
	}
	nodes, _, e := t.RedisCLI.ClusterNodes(n.Ip)
	if e != nil || nodes == nil || len(*nodes) <= 1 {
		return false
	}
	return true
}

func (t *TestLab) deletePod(pod corev1.Pod) error {
	if err := t.Client.Delete(context.Background(), &pod); err != nil && !strings.Contains(err.Error(), "not found") {
		return err
	}
	return nil
}

func (t *TestLab) newRedisClusterView() (*view.RedisClusterView, bool) {
	v := &view.RedisClusterView{}
	pods, e := t.getRedisClusterPods()
	if e != nil {
		return v, false
	}
	e = v.CreateView(pods, t.RedisCLI)
	if e != nil {
		return v, false
	}
	return v, true
}

func (t *TestLab) getRedisClusterPods() ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	matchingLabels := t.Cluster.Spec.PodLabelSelector
	err := t.Client.List(context.Background(), pods, client.InNamespace(t.Cluster.ObjectMeta.Namespace), client.MatchingLabels(matchingLabels))
	if err != nil {
		return nil, err
	}
	return pods.Items, nil
}
