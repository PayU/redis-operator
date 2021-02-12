package rediscli

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
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
	defaultRedisCliTimeout = 10 * time.Second
)

/*
 * executeCommand returns the exec command stdout and stderr response and an error
 * The error is non-nil if execution was unsuccessful, stderr is not empty or stdout
 * contains an error message
 */
func (r *RedisCLI) executeCommand(args []string) (string, string, error) {
	var stdout, stderr bytes.Buffer

	ctx, cancel := context.WithTimeout(context.Background(), defaultRedisCliTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "redis-cli", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return stdout.String(), stderr.String(), err
	}

	if err := cmd.Wait(); err != nil {
		if e, ok := err.(*exec.ExitError); ok {

			// If the process exited by itself, just return the error to the caller
			if e.Exited() {
				return stdout.String(), stderr.String(), e
			}

			// We know now that the process could be started, but didn't exit
			// by itself. Something must have killed it. If the context is done,
			// we can *assume* that it has been killed by the exec.Command.
			// Let's return ctx.Err() so our user knows that this *might* be
			// the case.

			select {
			case <-ctx.Done():
				return stdout.String(), stderr.String(), errors.Errorf("exec of %v failed with: %v", args, ctx.Err())
			default:
				return stdout.String(), stderr.String(), errors.Errorf("exec of %v failed with: %v", args, e)
			}
		}
		return stdout.String(), stderr.String(), err
	}

	stdOutput := strings.TrimSpace(stdout.String())
	errOutput := strings.TrimSpace(stderr.String())

	if errOutput != "" {
		return stdOutput, errOutput, errors.New(errOutput)
	}
	if stdOutput != "" && strings.Contains(strings.ToLower(stdOutput), "error:") {
		return stdOutput, stdOutput, errors.New(stdOutput)
	}
	return stdOutput, errOutput, nil
}

// ClusterCreate uses the '--cluster create' option on redis-cli to create a cluster using a list of nodes
func (r *RedisCLI) ClusterCreate(leaderIPs []string) (string, error) {
	var leaderAddrs []string
	for _, leaderIP := range leaderIPs {
		leaderAddrs = append(leaderAddrs, leaderIP+":6379")
	}
	args := append([]string{"--cluster", "create"}, leaderAddrs...)

	// this will run the command non-interactively
	args = append(args, "--cluster-yes")

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" {
		return stdout, errors.Errorf("Failed to execute cluster create (%v): %s | %s | %v", leaderAddrs, stdout, stderr, err)
	}
	return stdout, nil
}

func (r *RedisCLI) ClusterCheck(nodeIP string) (string, error) {
	args := []string{"--cluster", "check", nodeIP + ":6379"}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" {
		return stdout, errors.Errorf("Cluster check result: (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return stdout, nil
}

// AddFollower uses the '--cluster add-node' option on redis-cli to add a node to the cluster
// newNodeIP: IP of the follower that will join the cluster
// nodeIP: 		IP of a node in the cluster
// leaderID: 	Redis ID of the leader that the new follower will replicate
func (r *RedisCLI) AddFollower(newNodeIP string, nodeIP string, leaderID string) (string, error) {
	args := []string{"--cluster", "add-node", newNodeIP + ":6379", nodeIP + ":6379", "--cluster-slave", "--cluster-master-id", leaderID}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" {
		return stdout, errors.Errorf("Failed to execute cluster add node (%s, %s, %s): %s | %s | %v", newNodeIP, nodeIP, leaderID, stdout, stderr, err)
	}
	return stdout, nil
}

// DelNode uses the '--cluster del-node' option of redis-cli to remove a node from the cluster
// nodeIP: any node of the cluster
// nodeID: node that needs to be removed
func (r *RedisCLI) DelNode(nodeIP string, nodeID string) (string, error) {
	args := []string{"--cluster", "del-node", nodeIP + ":6379", nodeID}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.Contains(stdout, "[ERR]") || stderr != "" {
		return stdout, errors.Errorf("Failed to execute cluster del-node (%s, %s): %s | %s | %v", nodeIP, nodeID, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-info
func (r *RedisCLI) ClusterInfo(nodeIP string) (*RedisClusterInfo, error) {
	args := []string{"-h", nodeIP, "cluster", "info"}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" {
		return nil, errors.Errorf("Failed to execute CLUSTER INFO (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return NewRedisClusterInfo(stdout), nil
}

// https://redis.io/commands/info
func (r *RedisCLI) Info(nodeIP string) (*RedisInfo, error) {
	args := []string{"-h", nodeIP, "info"}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" {
		return nil, errors.Errorf("Failed to execute INFO (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return NewRedisInfo(stdout), nil
}

// https://redis.io/commands/ping
func (r *RedisCLI) Ping(nodeIP string, message ...string) (string, error) {
	args := []string{"-h", nodeIP, "ping"}
	if len(message) != 0 {
		args = append(args, message[0])
	}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" {
		return stdout, errors.Errorf("Failed to execute INFO (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return strings.TrimSpace(stdout), nil
}

// https://redis.io/commands/cluster-nodes
func (r *RedisCLI) ClusterNodes(nodeIP string) (*RedisClusterNodes, error) {
	args := []string{"-h", nodeIP, "cluster", "nodes"}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" {
		return nil, errors.Errorf("Failed to execute CLUSTER NODES(%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return NewRedisClusterNodes(stdout), nil
}

// https://redis.io/commands/cluster-myid
func (r *RedisCLI) MyClusterID(nodeIP string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "myid"}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" {
		return "", errors.Errorf("Failed to execute MYID(%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return strings.TrimSpace(stdout), nil
}

// ForgetNode command is used in order to remove a node, specified via its node ID, from the set of known nodes of the Redis Cluster node receiving the command.
// In other words the specified node is removed from the nodes table of the node receiving the command.
// https://redis.io/commands/cluster-forget
func (r *RedisCLI) ClusterForget(nodeIP string, forgetNodeID string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "forget", forgetNodeID}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER FORGET (%s, %s): %s | %s | %v", nodeIP, forgetNodeID, stdout, stderr, err)
	}
	return stdout, nil
}

// ClusterReplicas command provides a list of replica nodes replicating from a specified leader node
// https://redis.io/commands/cluster-replicas
func (r *RedisCLI) ClusterReplicas(nodeIP string, leaderNodeID string) (*RedisClusterNodes, error) {
	args := []string{"-h", nodeIP, "cluster", "replicas", leaderNodeID}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.Contains(stdout, "ERR") {
		return nil, errors.Errorf("Failed to execute CLUSTER REPLICAS (%s, %s): %s | %s | %v", nodeIP, leaderNodeID, stdout, stderr, err)
	}
	return NewRedisClusterNodes(stdout), err
}

// https://redis.io/commands/cluster-failover
func (r *RedisCLI) ClusterFailover(nodeIP string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "failover", "force"}

	if len(opt) != 0 {
		if strings.ToLower(opt[0]) != "force" && strings.ToLower(opt[0]) != "takeover" {
			r.Log.Info(fmt.Sprintf("Warning: CLUSTER FALOVER called with wrong option - %s", opt[0]))
		} else {
			args = append(args, opt[0])
		}
	}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER FAILOVER (%s, %v): %s | %s | %v", nodeIP, opt, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-meet
func (r *RedisCLI) ClusterMeet(nodeIP string, newNodeIP string, newNodePort string, newNodeBusPort ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "meet", newNodeIP, newNodePort}
	if len(newNodeBusPort) != 0 {
		args = append(args, newNodeBusPort[0])
	}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER MEET (%s, %s, %s, %v): %s | %s | %v", nodeIP, newNodeIP, newNodePort, newNodeBusPort, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-reset
func (r *RedisCLI) ClusterReset(nodeIP string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "reset"}
	if len(opt) != 0 {
		if strings.ToLower(opt[0]) != "hard" && strings.ToLower(opt[0]) != "soft" {
			r.Log.Info(fmt.Sprintf("Warning: CLUSTER RESET called with wrong option - %s", opt[0]))
		} else {
			args = append(args, opt[0])
		}
	}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER RESET (%s, %v): %s | %s | %v", nodeIP, opt, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/flushall
func (r *RedisCLI) Flushall(nodeIP string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "flushall"}
	if len(opt) != 0 {
		if strings.ToLower(opt[0]) != "async" {
			r.Log.Info(fmt.Sprintf("Warning: FLUSHALL called with wrong option - %s", opt[0]))
		} else {
			args = append(args, opt[0])
		}
	}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" {
		return stdout, errors.Errorf("Failed to execute FLUSHALL (%s, %v): %s | %s | %v", nodeIP, opt, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-replicate
func (r *RedisCLI) ClusterReplicate(nodeIP string, leaderID string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "replicate", leaderID}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER REPLICATE (%s, %s): %s | %s | %v", nodeIP, leaderID, stdout, stderr, err)
	}
	return stdout, nil
}
