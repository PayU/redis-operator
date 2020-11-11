package rediscli

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
)

type RedisCLI struct {
	Log logr.Logger
}

func NewRedisCLI(log logr.Logger) *RedisCLI {
	return &RedisCLI{
		Log: log,
	}
}

const (
	defaultRedisCliTimeout string = "5" // seconds
)

/*
 * executeCommand returns the exec command stdout response
 * or an error strcut in case something goes wrong
 */
func (r *RedisCLI) executeCommand(args []string) (string, error) {
	var stdout, stderr bytes.Buffer

	args = append([]string{defaultRedisCliTimeout, "redis-cli"}, args...)

	cmd := exec.Command("timeout", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	r.Log.Info(fmt.Sprintf("executing redis-cli command:%v", args[1:]))
	err := cmd.Run()
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("unexpected error occurred when executing redis-cli command:%s", stderr.String()))
		return "", err
	}

	r.Log.Info(fmt.Sprintf("redis-cli command output:%s", stdout.String()))
	return stdout.String(), nil
}

func (r *RedisCLI) ClusterCreate(leaderPodAddresses []string) error {
	r.Log.Info(fmt.Sprintf("initializing redis cluster. leader addresses:%v", leaderPodAddresses))
	args := append([]string{"--cluster", "create"}, leaderPodAddresses...)

	// this will run the command non-interactively
	args = append(args, "--cluster-yes")

	if _, err := r.executeCommand(args); err != nil {
		nodeIP := strings.Split(leaderPodAddresses[0], ":")
		clusterInfo, err := r.GetClusterInfo(nodeIP[0])
		if clusterInfo == nil {
			if err != nil {
				return err
			}
			return fmt.Errorf("Could not get redis cluster information")
		}
		if (*clusterInfo)["cluster_state"] == "ok" &&
			(*clusterInfo)["cluster_size"] == strconv.Itoa(len(leaderPodAddresses)) {
			r.Log.Info("Redis clustering complete")
			return nil
		} else {
			r.Log.Info("Redis clustering was NOT finished successfully")
			return fmt.Errorf("Redis clustering not ready (state: %s, size: %s)",
				(*clusterInfo)["cluster_state"], (*clusterInfo)["cluster_size"])
		}
	} else {
		return err
	}
}

func (r *RedisCLI) AddFollower(followerIP string, leaderIP string, redisLeaderID string) error {
	r.Log.Info(fmt.Sprintf("linkinig follower [%s] with leader [%s]", followerIP, redisLeaderID))
	args := []string{"--cluster", "add-node", followerIP + ":6379", leaderIP + ":6379", "--cluster-slave", "--cluster-master-id", redisLeaderID}
	_, err := r.executeCommand(args) // TODO: the stdout should be checked for errors
	if err != nil {
		r.Log.Info("unable to link follower [%s] and leader [%s]", followerIP, redisLeaderID)
		return err
	}
	return nil
}

func (r *RedisCLI) GetClusterInfo(nodeIP string) (*RedisClusterInfo, error) {
	r.Log.Info(fmt.Sprintf("retrieving cluster info from [%s]", nodeIP))
	args := []string{"-h", nodeIP, "cluster", "info"}

	stdout, err := r.executeCommand(args)
	if err != nil {
		r.Log.Info("unable to check cluster info using redis-cli")
		return nil, err
	}
	return NewRedisClusterInfo(stdout), nil
}

func (r *RedisCLI) GetInfo(nodeIP string) (*RedisInfo, error) {
	r.Log.Info(fmt.Sprintf("retrieving info from [%s]", nodeIP))
	args := []string{"-h", nodeIP, "info"}

	stdout, err := r.executeCommand(args)
	if err != nil {
		r.Log.Info("unable to check info using redis-cli")
		return nil, err
	}

	return NewRedisInfo(stdout), nil
}

func (r *RedisCLI) GetClusterNodesInfo(nodeIP string) (*RedisClusterNodes, error) {
	r.Log.Info(fmt.Sprintf("retrieving cluster nodes info from [%s]", nodeIP))
	args := []string{"-h", nodeIP, "cluster", "nodes"}

	stdout, err := r.executeCommand(args)
	if err != nil {
		r.Log.Info("unable to get cluster nodes using redis-cli")
		return nil, err
	}

	return NewRedisClusterNodes(stdout), nil
}

// https://redis.io/commands/cluster-myid
func (r *RedisCLI) GetMyClusterID(nodeIP string) (string, error) {
	r.Log.Info(fmt.Sprintf("retrieving cluster ID from [%s]", nodeIP))
	args := []string{"-h", nodeIP, "cluster", "myid"}

	stdout, err := r.executeCommand(args) // TODO: check stdout for errors
	if err != nil {
		r.Log.Error(err, "unable to get cluster nodes using redis-cli")
		return "", err
	}
	return strings.TrimSpace(stdout), nil
}

// ForgetNode command is used in order to remove a node, specified via its node ID, from the set of known nodes of the Redis Cluster node receiving the command.
// In other words the specified node is removed from the nodes table of the node receiving the command.
// https://redis.io/commands/cluster-forget
func (r *RedisCLI) ForgetNode(nodeIP string, forgetNodeID string) error {
	r.Log.Info(fmt.Sprintf("sending cluster forget command on [%s] node-ip. node-id to be forgotten [%s]", nodeIP, forgetNodeID))
	args := []string{"-h", nodeIP, "cluster", "forget", forgetNodeID}

	stdout, err := r.executeCommand(args)
	if strings.Contains(stdout, "Can't forget my master") {
		return fmt.Errorf(stdout)
	}

	if err != nil {
		return err
	}

	return nil
}

// GetLeaderReplicas ommand provides a list of replica nodes replicating from the specified leader node
// https://redis.io/commands/cluster-replicas
func (r *RedisCLI) GetLeaderReplicas(nodeIP string, leaderNodeID string) (*LeaderReplicas, error) {
	r.Log.Info(fmt.Sprintf("sending 'CLUSTER REPLICAS' command for leader [%s] on node-ip [%s]", leaderNodeID, nodeIP))
	args := []string{"-h", nodeIP, "cluster", "replicas", leaderNodeID}

	stdout, err := r.executeCommand(args)
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("unable to get cluster cluster replicas for [%s] when connection to node [%s]", leaderNodeID, nodeIP))
		return nil, err
	}

	return NewLeaderReplicas(stdout, r.Log), err
}
