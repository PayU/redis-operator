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

const IP_IDX = 0
const PORT_IDX = 1
const DEFAULT_REDIS_PORT = "6379"

type RedisAuth struct {
	User string
}

type RedisCLI struct {
	Log         logr.Logger
	Auth        *RedisAuth
	DefaultPort string
}

func NewRedisCLI(log *logr.Logger) *RedisCLI {
	return &RedisCLI{
		Log:         *log,
		Auth:        nil,
		DefaultPort: DEFAULT_REDIS_PORT,
	}
}

const (
	defaultRedisCliTimeout = 20 * time.Second
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

	if r.Auth != nil {
		args = append([]string{"--user", r.Auth.User}, args...)
	}
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

func (r *RedisCLI) validatePortOrSetDefault(address string) string {
	addrSplit := strings.Split(address, ":")
	if len(addrSplit) == 1 {
		address = addrSplit[IP_IDX] + ":" + r.DefaultPort
	}
	return address
}

func standardizeSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func argumentLineToArgMap(argumentLine string) map[string]string {
	argsValuesLine := strings.Split(standardizeSpaces(argumentLine), " ")
	argMap := make(map[string]string)
	skip := false
	for i, arg := range argsValuesLine {
		if !skip {
			if i+1 < len(argsValuesLine) {
				argMap[arg] = argsValuesLine[i+1]
			} else {
				argMap[arg] = ""
			}
		}
		skip = !skip
	}
	return argMap
}

func (r *RedisCLI) validatePortArgumentForRequestRout(argumentLine string) string {
	argMap := argumentLineToArgMap(argumentLine)
	if _, ok := argMap["-p"]; !ok {
		return "-p " + r.DefaultPort + " " + argumentLine
	}
	return argumentLine
}

// ClusterCreate uses the '--cluster create' option on redis-cli to create a cluster using a list of nodes
func (r *RedisCLI) ClusterCreate(leaderAddrses []string, opt ...string) (string, error) {
	for i, leaderAddrs := range leaderAddrses {
		leaderAddrses[i] = r.validatePortOrSetDefault(leaderAddrs)
	}
	args := append([]string{"--cluster", "create"}, leaderAddrses...)

	// this will run the command non-interactively
	args = append(args, "--cluster-yes")

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute cluster create (%v): %s | %s | %v", leaderAddrses, stdout, stderr, err)
	}
	return stdout, nil
}

func (r *RedisCLI) ClusterCheck(nodeAddr string, opt ...string) (string, error) {
	nodeAddr = r.validatePortOrSetDefault(nodeAddr)
	args := []string{"--cluster", "check", nodeAddr}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Cluster check result: (%s): %s | %s | %v", nodeAddr, stdout, stderr, err)
	}
	return stdout, nil
}

// AddFollower uses the '--cluster add-node' option on redis-cli to add a node to the cluster
// newNodeAddr:      Address of the follower that will join the cluster, in a form of <IP>:<Port>
// existingNodeAdrr: Address of existing node in the cluster, in a form of <IP>:<Port>
// leaderID: 	     Redis ID of the leader that the new follower will replicate
// In case IP addresses will be provided without a port, the default redis port will be used
func (r *RedisCLI) AddFollower(newNodeAddr string, existingNodeAdrr string, leaderID string, opt ...string) (string, error) {
	newNodeAddr = r.validatePortOrSetDefault(newNodeAddr)
	existingNodeAdrr = r.validatePortOrSetDefault(existingNodeAdrr)
	args := []string{"--cluster", "add-node", newNodeAddr, existingNodeAdrr, "--cluster-slave", "--cluster-master-id", leaderID}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute cluster add node (%s, %s, %s): %s | %s | %v", newNodeAddr, existingNodeAdrr, leaderID, stdout, stderr, err)
	}
	return stdout, nil
}

// DelNode uses the '--cluster del-node' option of redis-cli to remove a node from the cluster
// nodeAddr: node of interest from the cluster, address format is <IP>:<Port>
// nodeID: node that needs to be removed
// In case IP address will be provided without a port, the default redis port will be used
func (r *RedisCLI) DelNode(nodeAddr string, nodeID string, opt ...string) (string, error) {
	nodeAddr = r.validatePortOrSetDefault(nodeAddr)
	args := []string{"--cluster", "del-node", nodeAddr, nodeID}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || stderr != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute cluster del-node (%s, %s): %s | %s | %v", nodeAddr, nodeID, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-info
func (r *RedisCLI) ClusterInfo(nodeIP string, opt ...string) (*RedisClusterInfo, error) {
	args := []string{"-h", nodeIP, "cluster", "info"}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return nil, errors.Errorf("Failed to execute CLUSTER INFO (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return NewRedisClusterInfo(stdout), nil
}

// https://redis.io/commands/info
func (r *RedisCLI) Info(nodeIP string, opt ...string) (*RedisInfo, error) {
	args := []string{"-h", nodeIP, "info"}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return nil, errors.Errorf("Failed to execute INFO (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return NewRedisInfo(stdout), nil
}

// https://redis.io/commands/ping
func (r *RedisCLI) Ping(nodeIP string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "ping"}
	if len(opt) != 0 {
		args = append(args, opt[0])
	}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute INFO (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return strings.TrimSpace(stdout), nil
}

// https://redis.io/commands/cluster-nodes
func (r *RedisCLI) ClusterNodes(nodeIP string, opt ...string) (*RedisClusterNodes, error) {
	args := []string{"-h", nodeIP, "cluster", "nodes"}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return nil, errors.Errorf("Failed to execute CLUSTER NODES(%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return NewRedisClusterNodes(stdout), nil
}

// https://redis.io/commands/cluster-myid
func (r *RedisCLI) MyClusterID(nodeIP string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "myid"}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute MYID(%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return strings.TrimSpace(stdout), nil
}

// ForgetNode command is used in order to remove a node, specified via its node ID, from the set of known nodes of the Redis Cluster node receiving the command.
// In other words the specified node is removed from the nodes table of the node receiving the command.
// https://redis.io/commands/cluster-forget
func (r *RedisCLI) ClusterForget(nodeIP string, forgetNodeID string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "forget", forgetNodeID}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER FORGET (%s, %s): %s | %s | %v", nodeIP, forgetNodeID, stdout, stderr, err)
	}
	return stdout, nil
}

// ClusterReplicas command provides a list of replica nodes replicating from a specified leader node
// https://redis.io/commands/cluster-replicas
func (r *RedisCLI) ClusterReplicas(nodeIP string, leaderNodeID string, opt ...string) (*RedisClusterNodes, error) {
	args := []string{"-h", nodeIP, "cluster", "replicas", leaderNodeID}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return nil, errors.Errorf("Failed to execute CLUSTER REPLICAS (%s, %s): %s | %s | %v", nodeIP, leaderNodeID, stdout, stderr, err)
	}
	return NewRedisClusterNodes(stdout), err
}

// https://redis.io/commands/cluster-failover
func (r *RedisCLI) ClusterFailover(nodeIP string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "failover"}

	if len(opt) != 0 && opt[0] != "" {
		if strings.ToLower(opt[0]) != "force" && strings.ToLower(opt[0]) != "takeover" {
			r.Log.Info(fmt.Sprintf("Warning: CLUSTER FALOVER called with wrong option - %s", opt[0]))
		} else {
			args = append(args, opt[0])
		}
	}

	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER FAILOVER (%s, %v): %s | %s | %v", nodeIP, opt, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-meet
// nodeIP: IP address of our node of interest from the redis cluster
// newNodeAddr: address of the node that is requested to be met, address in the form of <IP>:<Port>
// In case IP address will be provided without a port, the default redis port will be used
func (r *RedisCLI) ClusterMeet(nodeIP string, newNodeAddr string, opt ...string) (string, error) {
	newNodeAddr = r.validatePortOrSetDefault(newNodeAddr)
	newAddr := strings.Split(newNodeAddr, ":")
	args := []string{"-h", nodeIP, "cluster", "meet", newAddr[IP_IDX], newAddr[PORT_IDX]}
	if len(opt) != 0 {
		args = append(args, opt[0])
	}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER MEET (%s, %s, %s, %v): %s | %s | %v", nodeIP, newAddr[IP_IDX], newAddr[PORT_IDX], opt, stdout, stderr, err)
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
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
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
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute FLUSHALL (%s, %v): %s | %s | %v", nodeIP, opt, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-replicate
func (r *RedisCLI) ClusterReplicate(nodeIP string, leaderID string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "replicate", leaderID}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER REPLICATE (%s, %s): %s | %s | %v", nodeIP, leaderID, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/acl-load
func (r *RedisCLI) ACLLoad(nodeIP string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "acl", "load"}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute ACL LOAD (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/acl-list
func (r *RedisCLI) ACLList(nodeIP string, opt ...string) (*RedisACL, error) {
	args := []string{"-h", nodeIP, "acl", "list"}
	stdout, stderr, err := r.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return nil, errors.Errorf("Failed to execute ACL LIST (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	acl, err := NewRedisACL(stdout)
	if err != nil {
		return nil, err
	}
	return acl, nil
}
