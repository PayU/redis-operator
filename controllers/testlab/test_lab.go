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
var fetchViewTimeOut = 1 * time.Minute

var clusterHealthCheckInterval = 10 * time.Second
var clusterHealthCheckTimeOutLimit = 5 * time.Minute

var randomChoiceRetries int = 4

var sleepPerTest time.Duration = 2 * time.Second
var sleepPerPodCheck time.Duration = 2 * time.Second
var sleepPerHealthCheck time.Duration = 5 * time.Second

var dataWriteRetries int = 5
var dataReadRetries int = 5

var intervalsBetweenWrites time.Duration = 500 * time.Millisecond

var totalDataWrites int = 200

var mutex = &sync.Mutex{}

func (t *TestLab) RunTest(nodes *map[string]*view.NodeStateView, withData bool) {
	t.Report = "\n[TEST LAB] Cluster test report:\n\n"
	isReady := t.waitForHealthyCluster(nodes)
	if !isReady {
		return
	}
	if withData {
		t.testSuitWithData(nodes)
	} else {
		t.testSuit(nodes)
	}
}

func (t *TestLab) testSuit(nodes *map[string]*view.NodeStateView) {
	test_1 := t.runTest(nodes, 1)
	if !test_1 {
		return
	}
	test_2 := t.runTest(nodes, 2)
	if !test_2 {
		return
	}
	test_3 := t.runTest(nodes, 3)
	if !test_3 {
		return
	}
	test_4 := t.runTest(nodes, 4)
	if !test_4 {
		return
	}
	test_5 := t.runTest(nodes, 5)
	if !test_5 {
		return
	}
	test_6 := t.runTest(nodes, 6)
	if !test_6 {
		return
	}
}

func (t *TestLab) testSuitWithData(nodes *map[string]*view.NodeStateView) {
	test_1 := t.runTestWithData(nodes, 1)
	if !test_1 {
		return
	}
	test_2 := t.runTestWithData(nodes, 2)
	if !test_2 {
		return
	}
	test_3 := t.runTestWithData(nodes, 3)
	if !test_3 {
		return
	}
	test_4 := t.runTestWithData(nodes, 4)
	if !test_4 {
		return
	}
	test_5 := t.runTestWithData(nodes, 5)
	if !test_5 {
		return
	}
	test_6 := t.runTestWithData(nodes, 6)
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
	t.Report += fmt.Sprintf("[TEST LAB] Reads success rate  : [%v%v]\n", readSuccessRate, "%")
}

func (t *TestLab) runTest(nodes *map[string]*view.NodeStateView, testNum int) bool {
	if t.RedisClusterClient == nil {
		return false
	}
	result := false
	switch testNum {
	case 1:
		result = t.test_delete_follower(nodes, testNum)
		break
	case 2:
		result = t.test_delete_leader(nodes, testNum)
		break
	case 3:
		result = t.test_delete_leader_and_follower(nodes, testNum)
		break
	case 4:
		result = t.test_delete_all_followers(nodes, testNum)
		break
	case 5:
		result = t.test_delete_all_azs_beside_one(nodes, testNum)
		break
	case 6:
		result = t.test_delete_leader_and_all_its_followers(nodes, testNum)
		break
	}
	return result
}

func (t *TestLab) runTestWithData(nodes *map[string]*view.NodeStateView, testNum int) bool {
	if t.RedisClusterClient == nil {
		return false
	}
	var wg sync.WaitGroup
	result := false
	sw := 0
	sr := 0
	data := map[string]string{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		switch testNum {
		case 1:
			result = t.test_delete_follower(nodes, testNum)
			break
		case 2:
			result = t.test_delete_leader(nodes, testNum)
			break
		case 3:
			result = t.test_delete_leader_and_follower(nodes, testNum)
			break
		case 4:
			result = t.test_delete_all_followers(nodes, testNum)
			break
		case 5:
			result = t.test_delete_all_azs_beside_one(nodes, testNum)
			break
		case 6:
			result = t.test_delete_leader_and_all_its_followers(nodes, testNum)
			break
		}
	}()
	go func() {
		defer wg.Done()
		sw, data = t.testDataWrites(totalDataWrites)
	}()
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

func (t *TestLab) test_delete_leader(nodes *map[string]*view.NodeStateView, testNum int) bool {
	time.Sleep(sleepPerTest)
	t.Log.Info("[TEST LAB] Running test: Delete leader...")
	t.Report += fmt.Sprintf("\n[TEST LAB] Test %v delete leader...", testNum)
	randomLeader := t.PickRandomeLeader(map[string]bool{}, nodes, randomChoiceRetries)
	result := t.test_delete_pods([]string{randomLeader}, nodes)
	t.Report += fmt.Sprintf("\n[TEST LAB] Test %v: delete leader result [%v]\n", testNum, result)
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
	mutex.Lock()
	info, _, err := t.RedisCLI.Info(nodeIP)
	mutex.Unlock()
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
		t.Report += fmt.Sprintf("\n[TEST LAB] Error while waiting for cluster to heal, probe intervals: [%v], probe timeout: [%v]", clusterHealthCheckInterval, clusterHealthCheckTimeOutLimit)
		return false
	}
	return isHealthyCluster
}

func (t *TestLab) isClusterAligned(expectedNodes *map[string]*view.NodeStateView) bool {
	v := t.WaitForClusterView()
	if v == nil {
		return false
	}
	t.Log.Info("[TEST LAB] Checking if cluster is ready...")
	for _, n := range *expectedNodes {
		time.Sleep(sleepPerPodCheck)
		node, exists := v.Nodes[n.Name]
		if !exists || node == nil {
			return false
		}
		if node.IsLeader {
			if !t.isLeaderAligned(node) {
				return false
			}
		} else {
			if !t.isFollowerAligned(node, expectedNodes) {
				return false
			}
		}
	}
	totalExpectedNodes := t.Cluster.Spec.LeaderCount * (t.Cluster.Spec.LeaderFollowersCount + 1)
	clusterOK := len(v.Nodes) == totalExpectedNodes && len(*expectedNodes) == totalExpectedNodes
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
	mutex.Lock()
	nodes, _, e := t.RedisCLI.ClusterNodes(n.Ip)
	mutex.Unlock()
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
