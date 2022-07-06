package controllers

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	rediscli "github.com/PayU/redis-operator/controllers/rediscli"
	"github.com/PayU/redis-operator/controllers/redisclient"
	"github.com/PayU/redis-operator/controllers/testlab"
	view "github.com/PayU/redis-operator/controllers/view"
	"github.com/labstack/echo/v4"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

/**
Set the operator state to RESET state.
In the next reconcile loop, the operator will enter a RESET mode, which will lead to the following steps:
1. Delete all redis cluster pods
2. Wait for all redis cluster pods to terminate
3. Create new redis cluster pods according to the spec
[WARN] This entry point is concidered sensitive, and is not allowed naturally. In order to enable it, the config param 'ExposeSensitiveEntryPoints' need to be set to 'true'.
**/
func DoResetCluster(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster reset action")
	}
	if reconciler.Config.Setters.ExposeSensitiveEntryPoints == false {
		return c.String(http.StatusOK, "Sensitive operation - Not allowed")
	}
	reconciler.Log.Info("[WARN] Sensitive entry point, on the way to pre-prod / prod environments, the access should be removed from router list")
	cluster.Status.ClusterState = string(Reset)
	reconciler.saveOperatorState(cluster)
	return c.String(http.StatusOK, "Set cluster state to reset mode")
}

/**
Triggers the redis-cli command CLUSTER REBALANCE
In case of failure, the cluster state will be set to ClusterFix, which will lead to a trigger of ClusterFix redis-cli command within the next reconcile loop
**/
func ClusterRebalance(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster rebalance action")
	}
	reconciler.saveClusterStateView(cluster)
	v, ok := reconciler.NewRedisClusterView(cluster)
	if !ok {
		return c.String(http.StatusOK, "Could not retrieve redis cluster view")
	}
	reconciler.removeSoloLeaders(v)
	healthyServerName, found := reconciler.findHealthyLeader(v)
	if !found {
		return c.String(http.StatusOK, "Could not find healthy server to serve the rebalance request")
	}
	mutex := &sync.Mutex{}
	mutex.Lock()
	reconciler.RedisClusterStateView.ClusterState = view.ClusterRebalance
	healthyServerIp := v.Nodes[healthyServerName].Ip
	reconciler.waitForAllNodesAgreeAboutSlotsConfiguration(v, nil)
	_, _, err := reconciler.RedisCLI.ClusterRebalance(healthyServerIp, true)
	if err != nil {
		reconciler.RedisClusterStateView.ClusterState = view.ClusterFix
		reconciler.Log.Error(err, "Could not perform cluster rebalance")
	}
	reconciler.RedisClusterStateView.ClusterState = view.ClusterOK
	mutex.Unlock()
	reconciler.saveClusterStateView(cluster)
	return c.String(http.StatusOK, "Cluster rebalance attempt executed")
}

/**
Triggers the redis-cli command CLUSTER FIX
In case of failure, the cluster state will remain ClusterFix, which will lead to a triger of additional attempt to fix in the next reconcile loop
In case of success, the cluster state will be set to ClusterReblance, which will lead to a trigger of redic-cli command CLUSTER REBALANCE in the next reconcile loop
**/
func ClusterFix(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster fix action")
	}
	reconciler.saveClusterStateView(cluster)
	v, ok := reconciler.NewRedisClusterView(cluster)
	if !ok {
		return c.String(http.StatusOK, "Could not retrieve redis cluster view")
	}
	healthyServerName, found := reconciler.findHealthyLeader(v)
	if !found {
		return c.String(http.StatusOK, "Could not find healthy server to serve the fix request")
	}
	healthyServerIp := v.Nodes[healthyServerName].Ip
	mutex := &sync.Mutex{}
	mutex.Lock()
	reconciler.RedisClusterStateView.ClusterState = view.ClusterFix
	_, _, err := reconciler.RedisCLI.ClusterFix(healthyServerIp)
	if err != nil {
		reconciler.Log.Error(err, "Could not perform cluster fix")
	}
	reconciler.RedisClusterStateView.ClusterState = view.ClusterRebalance
	reconciler.Log.Info("It is recommended to run rebalance after each cluster fix, changing state to [ClusterRebalance]")
	mutex.Unlock()
	reconciler.saveClusterStateView(cluster)
	return c.String(http.StatusOK, "Cluster fix attempt executed")
}

/**
Triggers an atomic flow of forgetting all redis cluster lost nodes: non-responsive nodes that still exists in the tables of some of the responsive ones.
**/
func ForgetLostNodes(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster forget lost nodes action")
	}
	v, ok := reconciler.NewRedisClusterView(cluster)
	if !ok {
		return c.String(http.StatusOK, "Could not retrieve redis cluster view")
	}
	reconciler.forgetLostNodes(cluster, v)
	return c.String(http.StatusOK, "Finish execution for attempt to forget lost nodes")
}

/**
Triggers reconcile loop by manual request.
Can be useful in case of event that prevents from manager to proceed with it's cluster maintenance routin
[WARN] Direct reconcile trigger might run the loop without enqueue it again causing the operator to not scheduling another run within requested time.
In case of need run eforced reconcile manually several times until recovery is complete, and restart manager when cluster is stable.
**/
func ForceReconcile(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster reconcile action")
	}
	reconciler.saveClusterStateView(cluster)
	_, err := reconciler.Reconcile(ctrl.Request{types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}})
	if err != nil {
		reconciler.Log.Error(err, "Could not perform reconcile trigger")
	}
	return c.String(http.StatusOK, "Force Reconcile request triggered, direct reconcile trigger might run the loop without enqueue it again causing the operator to not scheduling another run within requested time. " + 
	"\nIn case of need run eforced reconcile manually several times until recovery is complete, and restart manager when cluster is stable")
}

/**
Sets the value of parameter 'IsUpToDate' to false for each one of the reported nodes in the cluster state map.
In the next healthy reconcile loop, upgrade process will take part: each marked node will failover, forgotten, removed, deleted, re created and marked again with 'IsUpToDate' value of true
This process takes part moderately according to a suggested heuristic that relays on cluster size, and separates upgrade steps of leaders from upgrade steps of followers. 
**/
func UpgradeCluster(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster upgarde action")
	}
	for _, n := range reconciler.RedisClusterStateView.Nodes {
		n.IsUpToDate = false
	}
	requestUpgrade = false
	reconciler.saveClusterStateView(cluster)
	return c.String(http.StatusOK, "Cluster upgarde request triggered")
}

/**
Triggers a flow of testing routine that induces events with different severities in order to challenge the operator by simulating possible dissaster scenarios.
**/
func ClusterTest(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster test")
	}
	return c.String(http.StatusOK, setAndStartTestLab(&c, false))
}

/**
Triggers a flow of testing routine that induces events with different severities in order to challenge the operator by simulating possible dissaster scenarios.
The flow creates mock data and sends it to the redis cluster nodes, later attempts to report estimated possible data loss that might be expirienced during each dissaster scenario.
[WARN] This entry point is concidered sensitive, and is not allowed naturally. In order to enable it, the config param 'ExposeSensitiveEntryPoints' need to be set to 'true'.
**/
func ClusterTestWithData(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster test")
	}
	if reconciler.Config.Setters.ExposeSensitiveEntryPoints == false {
		return c.String(http.StatusOK, "Sensitive operation - Not allowed")
	}
	reconciler.Log.Info("[WARN] Sensitive entry point, on the way to pre-prod / prod environments, the access should be removed from router list")
	return c.String(http.StatusOK, setAndStartTestLab(&c, true))
}

/**
Populates the redis cluster nodes with mock data for debug purposes.
[WARN] This entry point is concidered sensitive, and is not allowed naturally. In order to enable it, the config param 'ExposeSensitiveEntryPoints' need to be set to 'true'.
**/
func PopulateClusterWithMockData(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster popluate data")
	}
	if reconciler.Config.Setters.ExposeSensitiveEntryPoints == false {
		return c.String(http.StatusOK, "Sensitive operation - Not allowed")
	}
	v, ok := reconciler.NewRedisClusterView(cluster)
	if !ok || v == nil {
		return c.String(http.StatusOK, "Could not perform cluster populate data")
	}
	reconciler.Log.Info("[WARN] Sensitive entry point, on the way to pre-prod / prod environments, the access should be removed from router list")
	redisCli := rediscli.NewRedisCLI(&reconciler.Log)
	user := os.Getenv("REDIS_USERNAME")
	if user != "" {
		redisCli.Auth = &rediscli.RedisAuth{
			User: user,
		}
	}
	clusterCli := redisclient.GetRedisClusterClient(v, redisCli)
	printUsedMemoryForAllNodes(v)

	total := 5000000
	init := 0
	sw := 0

	updateClientPer := 15000
	loopsBeforeClientUpdate := 0

	for i := init; i < init+total; i++ {
		key := "key" + fmt.Sprintf("%v", i)
		val := "val" + fmt.Sprintf("%v", i)
		err := clusterCli.Set(key, val, 3)
		if err == nil {
			sw++
		}
		loopsBeforeClientUpdate++
		if loopsBeforeClientUpdate == updateClientPer {
			v, ok := reconciler.NewRedisClusterView(cluster)
			if ok && v != nil {
				clusterCli = redisclient.GetRedisClusterClient(v, redisCli)
				loopsBeforeClientUpdate = 0
			}
		}
	}
	printUsedMemoryForAllNodes(v)
	return c.String(http.StatusOK, "Cluster populated with data")
}

/**
Flushes all the data of redis cluster nodes.
[WARN] This entry point is concidered sensitive, and is not allowed naturally. In order to enable it, the config param 'ExposeSensitiveEntryPoints' need to be set to 'true'.
**/
func FlushClusterData(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster flush data")
	}
	if reconciler.Config.Setters.ExposeSensitiveEntryPoints == false {
		return c.String(http.StatusOK, "Sensitive operation - Not allowed")
	}
	var cl *redisclient.RedisClusterClient = nil
	v, ok := reconciler.NewRedisClusterView(cluster)
	if !ok || v == nil {
		return c.String(http.StatusOK, "Could not perform cluster flush data")
	}
	reconciler.Log.Info("[WARN] Sensitive entry point, on the way to pre-prod / prod environments, the access should be removed from router list")
	cl = redisclient.GetRedisClusterClient(v, reconciler.RedisCLI)
	cl.FlushAllData()
	time.Sleep(10 * time.Second)
	printUsedMemoryForAllNodes(v)
	return c.String(http.StatusOK, "Cluster data flushed")
}

func setAndStartTestLab(c *echo.Context, data bool) string{
	cli := rediscli.NewRedisCLI(&reconciler.Log)
	user := os.Getenv("REDIS_USERNAME")
	if user != "" {
		cli.Auth = &rediscli.RedisAuth{
			User: user,
		}
	}
	t := &testlab.TestLab{
		Client:             reconciler.Client,
		RedisCLI:           cli,
		Cluster:            cluster,
		RedisClusterClient: nil,
		Log:                reconciler.Log,
		Report:             "",
	}
	t.RunTest(&reconciler.RedisClusterStateView.Nodes, data)
	return t.Report
}

func printUsedMemoryForAllNodes(v *view.RedisClusterView) {
	for _, n := range v.Nodes {
		printUsedMemory(n.Name, n.Ip)
	}
}

func printUsedMemory(name string, ip string) {
	info, _, err := reconciler.RedisCLI.Info(ip)
	if err != nil || info == nil {
		return
	}
	println(name + ": " + info.Memory["used_memory_human"])
}