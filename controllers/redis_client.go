package controllers

import "github.com/go-redis/redis"

var redisClient *redis.ClusterClient

func (r *RedisOperatorReconciler) initRedisClient(leaderAddrs []string) {
	r.Log.Info("initializing redis client")
	redisClient = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: leaderAddrs,
	})

	redisClient.Ping()
}

func executeCommand() {

}

func groupMasterNodesAsCluster(addresses []string) {

}
