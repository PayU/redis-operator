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

func ClusterTest(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster test")
	}
	return c.String(http.StatusOK, setAndStartTestLab(&c, false))
}

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

// [WARN] Should not appear on router when we go to prod
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