package controllers

import (
	"fmt"

	"github.com/go-redis/redis"
)

var redisClient *redis.ClusterClient

func (r *RedisOperatorReconciler) initRedisClient(leaderPodIPAddresses []string) {
	r.Log.Info(fmt.Sprintf("initializing redis client. leader addresses:%v", leaderPodIPAddresses))
	redisClient = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: leaderPodIPAddresses,
	})

	status := redisClient.Ping()

	r.Log.Info(status.Result())
	r.Log.Info(status.String())
	r.Log.Info(status.Err().Error())

	r.Log.Info("redis client was initialized successfully")
}

func executeCommand() {

}

func groupMasterNodesAsCluster(addresses []string) {

}
