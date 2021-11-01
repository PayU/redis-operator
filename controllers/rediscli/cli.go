package rediscli

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
)

const (
	defaultRedisCliTimeout        = 20 * time.Second
	REDIS_DEFAULT_PORT     string = "6379"
)

type RedisAuth struct {
	User string
}

type CommandHandler interface {
	buildCommand(routingPort string, args []string, auth *RedisAuth, opt ...string) []string
	executeCommand(args []string) (string, string, error)
}

type RunTimeCommandHandler struct{}

type RedisCLI struct {
	Log     logr.Logger
	Auth    *RedisAuth
	Port    string
	Handler CommandHandler
}

func NewRedisCLI(log *logr.Logger) *RedisCLI {
	return &RedisCLI{
		Log:     *log,
		Auth:    nil,
		Port:    REDIS_DEFAULT_PORT,
		Handler: &RunTimeCommandHandler{},
	}
}

func (h *RunTimeCommandHandler) buildCommand(routingPort string, args []string, auth *RedisAuth, opt ...string) []string {
	if auth != nil {
		args = append([]string{"--user", auth.User}, args...)
	}
	routingPort, opt = routingPortDecider(routingPort, opt)
	args = append([]string{"-p", routingPort}, args...)
	if len(opt) > 0 {
		args = append(args, opt...)
	}
	return args
}

/* Executes command and returns cmd stdout, stderr and runtime error if appears
 *  args: arguments, flags and their values, in the order they should appear as if they were executed in the cli itself
 */
func (h *RunTimeCommandHandler) executeCommand(args []string) (string, string, error) {

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

// Helpers

func routingPortDecider(cliDefaultPort string, opt []string) (string, []string) {
	port := cliDefaultPort
	if len(opt) > 0 {
		var optionalArgs []string
		port := cliDefaultPort
		portExtracted := false
		for _, arg := range opt {
			if !portExtracted {
				flag := "-p\\s+"
				flagVal := "\\d+"
				portArgRegex := flag + flagVal
				match, _ := regexp.MatchString(portArgRegex, arg)
				if match {
					comp, _ := regexp.Compile(portArgRegex)
					portArg := comp.FindStringSubmatch(arg)[0]
					port = regexp.MustCompile(flag).ReplaceAllString(portArg, "")
					arg = regexp.MustCompile("\\s*"+portArgRegex+"\\s*").ReplaceAllString(arg, " ")
					portExtracted = true
				}
			}
			trimmed := strings.TrimSpace(arg)
			if trimmed != "" {
				optionalArgs = append(optionalArgs, strings.TrimSpace(arg))
			}
		}
		for i, arg := range optionalArgs {
			trimmed := strings.TrimSpace(arg)
			if trimmed != "" {
				optionalArgs[i] = strings.TrimSpace(arg)
			}
		}
		return port, optionalArgs
	}
	return port, opt
}

// Receives: address in a format of <ip>:<port> or <ip>: or <ip>
// Returns: full address in a format of <ip>:<port>
//          if port was provided, the exact address will be returned. otherwise the returned address will be <ip>:<default cli port>
func addressPortDecider(address string, cliDefaultPort string) string {
	addr := strings.Split(address, ":")
	if len(addr) == 1 || (len(addr) == 2 && addr[1] == "") {
		return addr[0] + ":" + cliDefaultPort
	}
	return address
}

func addressesPortDecider(addresses []string, cliDefaultPort string) []string {
	var updatedAddresses []string
	for _, addr := range addresses {
		updatedAddresses = append(updatedAddresses, addressPortDecider(addr, cliDefaultPort))
	}
	return updatedAddresses
}

// End of Helpers

// ClusterCreate uses the '--cluster create' option on redis-cli to create a cluster using a list of nodes
func (r *RedisCLI) ClusterCreate(leadersAddresses []string, opt ...string) (string, error) {
	fullAddresses := addressesPortDecider(leadersAddresses, r.Port)
	args := append([]string{"--cluster", "create"}, fullAddresses...)
	args = append(args, "--cluster-yes") // this will run the command non-interactively
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute cluster create (%v): %s | %s | %v", fullAddresses, stdout, stderr, err)
	}
	return stdout, nil
}

func (r *RedisCLI) ClusterCheck(nodeAddr string, opt ...string) (string, error) {
	args := []string{"--cluster", "check", addressPortDecider(nodeAddr, r.Port)}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Cluster check result: (%s): %s | %s | %v", nodeAddr, stdout, stderr, err)
	}
	return stdout, nil
}

// AddFollower uses the '--cluster add-node' option on redis-cli to add a node to the cluster
// newNodeAddr: Address of the follower that will join the cluster in a format of <ip>:<port> or <ip>: or <ip>
// existingNodeAddr: IP of a node in the cluster in a format of <ip>:<port> or <ip>: or <ip>
// leaderID: 	Redis ID of the leader that the new follower will replicate
// In case port won't bw provided as part of the given addresses, cli default port will be added automatically to the address
func (r *RedisCLI) AddFollower(newNodeAddr string, existingNodeAddr string, leaderID string, opt ...string) (string, error) {
	args := []string{"--cluster", "add-node", addressPortDecider(newNodeAddr, r.Port), addressPortDecider(existingNodeAddr, r.Port), "--cluster-slave", "--cluster-master-id", leaderID}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute cluster add node (%s, %s, %s): %s | %s | %v", newNodeAddr, existingNodeAddr, leaderID, stdout, stderr, err)
	}
	return stdout, nil
}

// DelNode uses the '--cluster del-node' option of redis-cli to remove a node from the cluster
// nodeIP: any node of the cluster
// nodeID: node that needs to be removed
func (r *RedisCLI) DelNode(nodeIP string, nodeID string, opt ...string) (string, error) {
	args := []string{"--cluster", "del-node", addressPortDecider(nodeIP, r.Port), nodeID}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || stderr != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute cluster del-node (%s, %s): %s | %s | %v", nodeIP, nodeID, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-info
func (r *RedisCLI) ClusterInfo(nodeIP string, opt ...string) (*RedisClusterInfo, string, error) {

	args := []string{"-h", nodeIP, "cluster", "info"}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return nil, "", errors.Errorf("Failed to execute CLUSTER INFO (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return NewRedisClusterInfo(stdout), stdout, nil
}

// https://redis.io/commands/info
func (r *RedisCLI) Info(nodeIP string, opt ...string) (*RedisInfo, string, error) {

	args := []string{"-h", nodeIP, "info"}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return nil, "", errors.Errorf("Failed to execute INFO (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return NewRedisInfo(stdout), stdout, nil
}

// https://redis.io/commands/ping
func (r *RedisCLI) Ping(nodeIP string, message ...string) (string, error) {
	args := []string{"-h", nodeIP, "ping"}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, message...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute INFO (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return strings.TrimSpace(stdout), nil
}

// https://redis.io/commands/cluster-nodes
func (r *RedisCLI) ClusterNodes(nodeIP string, opt ...string) (*RedisClusterNodes, string, error) {

	args := []string{"-h", nodeIP, "cluster", "nodes"}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return nil, "", errors.Errorf("Failed to execute CLUSTER NODES(%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return NewRedisClusterNodes(stdout), stdout, nil
}

// https://redis.io/commands/cluster-myid
func (r *RedisCLI) MyClusterID(nodeIP string, opt ...string) (string, error) {

	args := []string{"-h", nodeIP, "cluster", "myid"}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
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
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER FORGET (%s, %s): %s | %s | %v", nodeIP, forgetNodeID, stdout, stderr, err)
	}
	return stdout, nil
}

// ClusterReplicas command provides a list of replica nodes replicating from a specified leader node
// https://redis.io/commands/cluster-replicas
func (r *RedisCLI) ClusterReplicas(nodeIP string, leaderNodeID string, opt ...string) (*RedisClusterNodes, string, error) {
	args := []string{"-h", nodeIP, "cluster", "replicas", leaderNodeID}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return nil, "", errors.Errorf("Failed to execute CLUSTER REPLICAS (%s, %s): %s | %s | %v", nodeIP, leaderNodeID, stdout, stderr, err)
	}
	return NewRedisClusterNodes(stdout), stdout, err
}

// https://redis.io/commands/cluster-failover
func (r *RedisCLI) ClusterFailover(nodeIP string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "failover"}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	clusterFailoverFlag := false
	for _, arg := range args {
		if clusterFailoverFlag {
			break
		}
		toLower := strings.ToLower(arg)
		if strings.Contains(toLower, "force") || strings.Contains(toLower, "takeover") {
			clusterFailoverFlag = true
		}
	}
	if !clusterFailoverFlag {
		if r.Log != nil {
			r.Log.Info(fmt.Sprintf("Warning: CLUSTER FAILOVER called with wrong option, argument list - %s", args))
		}
	}
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER FAILOVER (%s, %v): %s | %s | %v", nodeIP, opt, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-meet
func (r *RedisCLI) ClusterMeet(nodeIP string, newNodeIP string, newNodePort string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "meet", newNodeIP, newNodePort}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER MEET (%s, %s, %s, %v): %s | %s | %v", nodeIP, newNodeIP, newNodePort, opt, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-reset
func (r *RedisCLI) ClusterReset(nodeIP string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "reset"}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	clusterResetFlag := false
	for _, arg := range args {
		if clusterResetFlag {
			break
		}
		toLower := strings.ToLower(arg)
		if strings.Contains(toLower, "hard") || strings.Contains(toLower, "soft") {
			clusterResetFlag = true
		}
	}
	if !clusterResetFlag {
		if r.Log != nil {
			r.Log.Info(fmt.Sprintf("Warning: CLUSTER RESET called with wrong option, argument list - %s", args))
		}
	}
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER RESET (%s, %v): %s | %s | %v", nodeIP, opt, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/flushall
func (r *RedisCLI) Flushall(nodeIP string, opt ...string) (string, error) {

	args := []string{"-h", nodeIP, "flushall"}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	flushAllFlag := false
	for _, arg := range opt {
		if flushAllFlag {
			break
		}
		if strings.Contains(strings.ToLower(arg), "async") {
			flushAllFlag = true
		}
	}
	if !flushAllFlag {
		if r.Log != nil {
			r.Log.Info(fmt.Sprintf("Warning: FLUSHALL called with wrong option, argument list - %s", args))
		}
	}

	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute FLUSHALL (%s, %v): %s | %s | %v", nodeIP, opt, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-replicate
func (r *RedisCLI) ClusterReplicate(nodeIP string, leaderID string, opt ...string) (string, error) {

	args := []string{"-h", nodeIP, "cluster", "replicate", leaderID}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER REPLICATE (%s, %s): %s | %s | %v", nodeIP, leaderID, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/acl-load
func (r *RedisCLI) ACLLoad(nodeIP string, opt ...string) (string, error) {

	args := []string{"-h", nodeIP, "acl", "load"}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute ACL LOAD (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/acl-list
func (r *RedisCLI) ACLList(nodeIP string, opt ...string) (*RedisACL, string, error) {

	args := []string{"-h", nodeIP, "acl", "list"}
	args = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return nil, "", errors.Errorf("Failed to execute ACL LIST (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	acl, err := NewRedisACL(stdout)
	if err != nil {
		return nil, "", err
	}
	return acl, stdout, nil
}
