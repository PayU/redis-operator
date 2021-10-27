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

const IP_IDX = 0
const PORT_IDX = 1
const DEFAULT_REDIS_PORT = "6379"

type RedisAuth struct {
	User string
}

type CommandHandler struct {
	Command string
	ExecMsg string
	ErrMsg  string
	Error   error
}

type CommandHandlerProvider struct{}

type RedisCLI struct {
	Log         logr.Logger
	Auth        *RedisAuth
	DefaultPort string
	Provider    *CommandHandlerProvider
}

func (p *CommandHandlerProvider) provideCommandHandler() *CommandHandler {
	return &CommandHandler{}
}

func NewRedisCLI(log *logr.Logger) *RedisCLI {
	return &RedisCLI{
		Log:         *log,
		Auth:        nil,
		DefaultPort: DEFAULT_REDIS_PORT,
		Provider:    &CommandHandlerProvider{},
	}
}

const (
	defaultRedisCliTimeout = 20 * time.Second
)

func (h *CommandHandler) buildCommand(args []string, commandRoutingPort string, user string) {
	args = append([]string{"--user", user}, args...)
	args = append([]string{"-p", commandRoutingPort}, args...)

	for _, arg := range args {
		h.Command += " " + arg
	}
}

/*
 * executeCommand returns the exec command stdout and stderr response and an error
 * The error is non-nil if execution was unsuccessful, stderr is not empty or stdout
 * contains an error message
 */
func (h *CommandHandler) executeCommand() {
	// TODO: change signature to one that does not need to be provided with port
	var stdout, stderr bytes.Buffer

	ctx, cancel := context.WithTimeout(context.Background(), defaultRedisCliTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "redis-cli", h.Command)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		h.ExecMsg = stdout.String()
		h.ErrMsg = stderr.String()
		h.Error = err
		return
	}

	if err := cmd.Wait(); err != nil {
		h.ExecMsg = stdout.String()
		h.ErrMsg = stderr.String()
		if e, ok := err.(*exec.ExitError); ok {

			// If the process exited by itself, just return the error to the caller
			if e.Exited() {
				h.Error = err
				return
			}

			// We know now that the process could be started, but didn't exit
			// by itself. Something must have killed it. If the context is done,
			// we can *assume* that it has been killed by the exec.Command.
			// Let's return ctx.Err() so our user knows that this *might* be
			// the case.

			select {
			case <-ctx.Done():
				h.Error = errors.Errorf("exec of %v failed with: %v", h.Command, ctx.Err())
				return
			default:
				h.Error = errors.Errorf("exec of %v failed with: %v", h.Command, e)
				return
			}
		}
		h.Error = err
		return
	}

	h.ExecMsg = strings.TrimSpace(stdout.String())
	h.ErrMsg = strings.TrimSpace(stderr.String())
	h.Error = nil

	if h.ErrMsg != "" {
		h.Error = errors.New(h.ErrMsg)
	} else if h.ExecMsg != "" && strings.Contains(strings.ToLower(h.ExecMsg), "error:") {
		h.Error = errors.New(h.ExecMsg)
	}
}

func (r *RedisCLI) validatePortOrSetDefault(address string) string {
	addrSplit := strings.Split(address, ":")
	if len(addrSplit) == 1 {
		address = addrSplit[IP_IDX] + ":" + r.DefaultPort
	}
	return address
}

func standardizeSpaces(s []string) string {
	argsLine := ""
	for _, str := range s {
		argsLine += " " + strings.Join(strings.Fields(str), " ")
	}
	return argsLine
}

func argumentLineToArgMap(argumentLine []string) map[string]string {
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

func (r *RedisCLI) validateOptionalPortArg(argumentLine []string) (bool, string) {
	argMap := argumentLineToArgMap(argumentLine)
	if _, ok := argMap["-p"]; ok {
		return true, argMap["-p"]
	}
	return false, ""
}

func removeOptionalPortArgFromCommandLine(argLine string) string {
	regex := " -p \\d+ "
	matcher := regexp.MustCompile(regex)
	return matcher.ReplaceAllString(argLine, " ")
}

func (r *RedisCLI) getRoutingPortFromCommandLine(opt []string) (string, string) {
	routingPort := r.DefaultPort
	optinalArgsLine := ""
	if len(opt) > 0 {
		portProvided, port := r.validateOptionalPortArg(opt)
		if portProvided {
			routingPort = port
		}
		optinalArgsLine = removeOptionalPortArgFromCommandLine(standardizeSpaces(opt))
	}
	return routingPort, optinalArgsLine
}

// ClusterCreate uses the '--cluster create' option on redis-cli to create a cluster using a list of nodes
func (r *RedisCLI) ClusterCreate(leadersAddr []string, opt ...string) *CommandHandler {
	for i, leaderAddrs := range leadersAddr {
		leadersAddr[i] = r.validatePortOrSetDefault(leaderAddrs)
	}
	args := []string{"--cluster", "create"}
	args = append(args, leadersAddr...)

	// this will run the command non-interactively
	args = append(args, "--cluster-yes")

	routingPort, optinalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args = append(args, optinalArgsLine)

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || IsError(strings.TrimSpace(h.ExecMsg)) {
		h.Error = errors.Errorf("Failed to execute cluster create (%v): %s | %s | %v", leadersAddr, h.ExecMsg, h.ErrMsg, h.Error)
	}
	return h
}

func (r *RedisCLI) ClusterCheck(nodeAddr string, opt ...string) *CommandHandler {
	nodeAddr = r.validatePortOrSetDefault(nodeAddr)
	routingPort, optionalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args := []string{"--cluster", "check", nodeAddr, optionalArgsLine}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || IsError(strings.TrimSpace(h.ExecMsg)) {
		h.Error = errors.Errorf("Cluster check result: (%s): %s | %s | %v", nodeAddr, h.ExecMsg, h.ErrMsg, h.Error)
	}
	return h
}

// AddFollower uses the '--cluster add-node' option on redis-cli to add a node to the cluster
// newNodeAddr:      Address of the follower that will join the cluster, in a form of <IP>:<Port>
// existingNodeAdrr: Address of existing node in the cluster, in a form of <IP>:<Port>
// leaderID: 	     Redis ID of the leader that the new follower will replicate
// In case IP addresses will be provided without a port, the default redis port will be used
func (r *RedisCLI) AddFollower(newNodeAddr string, existingNodeAdrr string, leaderID string, opt ...string) *CommandHandler {
	newNodeAddr = r.validatePortOrSetDefault(newNodeAddr)
	existingNodeAdrr = r.validatePortOrSetDefault(existingNodeAdrr)
	routingPort, optionalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args := []string{"--cluster", "add-node", newNodeAddr, existingNodeAdrr, "--cluster-slave", "--cluster-master-id", leaderID, optionalArgsLine}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || IsError(strings.TrimSpace(h.ExecMsg)) {
		h.Error = errors.Errorf("Failed to execute cluster add node (%s, %s, %s): %s | %s | %v", newNodeAddr, existingNodeAdrr, leaderID, h.ExecMsg, h.ErrMsg, h.Error)
	}
	return h
}

// DelNode uses the '--cluster del-node' option of redis-cli to remove a node from the cluster
// nodeAddr: node of interest from the cluster, address format is <IP>:<Port>
// nodeID: node that needs to be removed
// In case IP address will be provided without a port, the default redis port will be used
func (r *RedisCLI) DelNode(nodeAddr string, nodeID string, opt ...string) *CommandHandler {
	nodeAddr = r.validatePortOrSetDefault(nodeAddr)
	routingPort, optinalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args := []string{"--cluster", "del-node", nodeAddr, nodeID, optinalArgsLine}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || IsError(strings.TrimSpace(h.ExecMsg)) {
		h.Error = errors.Errorf("Failed to execute cluster del-node (%s, %s): %s | %s | %v", nodeAddr, nodeID, h.ExecMsg, h.ErrMsg, h.Error)
	}
	return h
}

// https://redis.io/commands/cluster-info
func (r *RedisCLI) ClusterInfo(nodeIP string, opt ...string) (*CommandHandler, *RedisClusterInfo) {
	routingPort, optinalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args := []string{"-h", nodeIP, "cluster", "info", optinalArgsLine}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || IsError(strings.TrimSpace(h.ExecMsg)) {
		h.Error = errors.Errorf("Failed to execute CLUSTER INFO (%s): %s | %s | %v", nodeIP, h.ExecMsg, h.ErrMsg, h.Error)
		return h, nil
	}
	return h, NewRedisClusterInfo(h.ExecMsg)
}

// https://redis.io/commands/info
func (r *RedisCLI) Info(nodeIP string, opt ...string) (*CommandHandler, *RedisInfo) {
	routingPort, optinalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args := []string{"-h", nodeIP, "info", optinalArgsLine}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || IsError(strings.TrimSpace(h.ExecMsg)) {
		h.Error = errors.Errorf("Failed to execute INFO (%s): %s | %s | %v", nodeIP, h.ExecMsg, h.ErrMsg, h.Error)
		return h, nil
	}
	return h, NewRedisInfo(h.ExecMsg)
}

// https://redis.io/commands/ping
func (r *RedisCLI) Ping(nodeIP string, opt ...string) *CommandHandler {
	routingPort, optinalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args := []string{"-h", nodeIP, "ping", optinalArgsLine}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || IsError(strings.TrimSpace(h.ExecMsg)) {
		h.Error = errors.Errorf("Failed to execute INFO (%s): %s | %s | %v", nodeIP, h.ExecMsg, h.ErrMsg, h.Error)
	}
	return h
}

// https://redis.io/commands/cluster-nodes
func (r *RedisCLI) ClusterNodes(nodeIP string, opt ...string) (*CommandHandler, *RedisClusterNodes) {
	routingPort, optinalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args := []string{"-h", nodeIP, "cluster", "nodes", optinalArgsLine}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || IsError(strings.TrimSpace(h.ExecMsg)) {
		h.Error = errors.Errorf("Failed to execute CLUSTER NODES(%s): %s | %s | %v", nodeIP, h.ExecMsg, h.ErrMsg, h.Error)
		return h, nil
	}
	return h, NewRedisClusterNodes(h.ExecMsg)
}

// https://redis.io/commands/cluster-myid
func (r *RedisCLI) MyClusterID(nodeIP string, opt ...string) *CommandHandler {
	routingPort, optinalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args := []string{"-h", nodeIP, "cluster", "myid", optinalArgsLine}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || IsError(strings.TrimSpace(h.ExecMsg)) {
		h.Error = errors.Errorf("Failed to execute MYID(%s): %s | %s | %v", nodeIP, h.ExecMsg, h.ErrMsg, h.Error)
	}
	return h
}

// ForgetNode command is used in order to remove a node, specified via its node ID, from the set of known nodes of the Redis Cluster node receiving the command.
// In other words the specified node is removed from the nodes table of the node receiving the command.
// https://redis.io/commands/cluster-forget
func (r *RedisCLI) ClusterForget(nodeIP string, forgetNodeID string, opt ...string) *CommandHandler {
	routingPort, optinalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args := []string{"-h", nodeIP, "cluster", "forget", forgetNodeID, optinalArgsLine}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || strings.TrimSpace(h.ExecMsg) != "OK" {
		h.Error = errors.Errorf("Failed to execute CLUSTER FORGET (%s, %s): %s | %s | %v", nodeIP, forgetNodeID, h.ExecMsg, h.ErrMsg, h.Error)
	}
	return h
}

// ClusterReplicas command provides a list of replica nodes replicating from a specified leader node
// https://redis.io/commands/cluster-replicas
func (r *RedisCLI) ClusterReplicas(nodeIP string, leaderNodeID string, opt ...string) (*CommandHandler, *RedisClusterNodes) {
	routingPort, optionalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args := []string{"-h", nodeIP, "cluster", "replicas", leaderNodeID, optionalArgsLine}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || IsError(strings.TrimSpace(h.ExecMsg)) {
		h.Error = errors.Errorf("Failed to execute CLUSTER REPLICAS (%s, %s): %s | %s | %v", nodeIP, leaderNodeID, h.ExecMsg, h.ErrMsg, h.Error)
		return h, nil
	}
	return h, NewRedisClusterNodes(h.ExecMsg)
}

// https://redis.io/commands/cluster-failover
func (r *RedisCLI) ClusterFailover(nodeIP string, opt ...string) *CommandHandler {
	routingPort, _ := r.getRoutingPortFromCommandLine(opt)
	args := []string{"-h", nodeIP, "cluster", "failover"}

	if len(opt) != 0 && opt[0] != "" {
		if strings.ToLower(opt[0]) != "force" && strings.ToLower(opt[0]) != "takeover" {
			r.Log.Info(fmt.Sprintf("Warning: CLUSTER FALOVER called with wrong option - %s", opt[0]))
		} else {
			args = append(args, opt[0])
		}
	}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || strings.TrimSpace(h.ExecMsg) != "OK" {
		h.Error = errors.Errorf("Failed to execute CLUSTER FAILOVER (%s, %v): %s | %s | %v", nodeIP, opt, h.ExecMsg, h.ErrMsg, h.Error)
	}
	return h
}

// https://redis.io/commands/cluster-meet
// nodeIP: IP address of our node of interest from the redis cluster
// newNodeAddr: address of the node that is requested to be met, address in the form of <IP>:<Port>
// In case IP address will be provided without a port, the default redis port will be used
func (r *RedisCLI) ClusterMeet(nodeIP string, newNodeAddr string, opt ...string) *CommandHandler {

	newNodeAddr = r.validatePortOrSetDefault(newNodeAddr)
	newAddr := strings.Split(newNodeAddr, ":")
	routingPort, optionalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args := []string{"-h", nodeIP, "cluster", "meet", newAddr[IP_IDX], newAddr[PORT_IDX], optionalArgsLine}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || strings.TrimSpace(h.ExecMsg) != "OK" {
		h.Error = errors.Errorf("Failed to execute CLUSTER MEET (%s, %s, %s, %v): %s | %s | %v", nodeIP, newAddr[IP_IDX], newAddr[PORT_IDX], opt, h.ExecMsg, h.ErrMsg, h.Error)
	}
	return h
}

// https://redis.io/commands/cluster-reset
func (r *RedisCLI) ClusterReset(nodeIP string, opt ...string) *CommandHandler {
	routingPort, _ := r.getRoutingPortFromCommandLine(opt)
	args := []string{"-h", nodeIP, "cluster", "reset"}
	if len(opt) != 0 {
		if strings.ToLower(opt[0]) != "hard" && strings.ToLower(opt[0]) != "soft" {
			r.Log.Info(fmt.Sprintf("Warning: CLUSTER RESET called with wrong option - %s", opt[0]))
		} else {
			args = append(args, opt[0])
		}
	}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || strings.TrimSpace(h.ExecMsg) != "OK" {
		h.Error = errors.Errorf("Failed to execute CLUSTER RESET (%s, %v): %s | %s | %v", nodeIP, opt, h.ExecMsg, h.ErrMsg, h.Error)
	}
	return h
}

// https://redis.io/commands/flushall
func (r *RedisCLI) Flushall(nodeIP string, opt ...string) *CommandHandler {
	routingPort, _ := r.getRoutingPortFromCommandLine(opt)
	args := []string{"-h", nodeIP, "flushall"}
	if len(opt) != 0 {
		if strings.ToLower(opt[0]) != "async" {
			r.Log.Info(fmt.Sprintf("Warning: FLUSHALL called with wrong option - %s", opt[0]))
		} else {
			args = append(args, opt[0])
		}
	}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || IsError(strings.TrimSpace(h.ExecMsg)) {
		h.Error = errors.Errorf("Failed to execute FLUSHALL (%s, %v): %s | %s | %v", nodeIP, opt, h.ExecMsg, h.ErrMsg, h.Error)
	}
	return h
}

// https://redis.io/commands/cluster-replicate
func (r *RedisCLI) ClusterReplicate(nodeIP string, leaderID string, opt ...string) *CommandHandler {
	routingPort, optionalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args := []string{"-h", nodeIP, "cluster", "replicate", leaderID, optionalArgsLine}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || strings.TrimSpace(h.ExecMsg) != "OK" {
		h.Error = errors.Errorf("Failed to execute CLUSTER REPLICATE (%s, %s): %s | %s | %v", nodeIP, leaderID, h.ExecMsg, h.ErrMsg, h.Error)
	}
	return h
}

// https://redis.io/commands/acl-load
func (r *RedisCLI) ACLLoad(nodeIP string, opt ...string) *CommandHandler {
	routingPort, optionalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args := []string{"-h", nodeIP, "acl", "load", optionalArgsLine}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || strings.TrimSpace(h.ExecMsg) != "OK" {
		h.Error = errors.Errorf("Failed to execute ACL LOAD (%s): %s | %s | %v", nodeIP, h.ExecMsg, h.ErrMsg, h.Error)
	}
	return h
}

// https://redis.io/commands/acl-list
func (r *RedisCLI) ACLList(nodeIP string, opt ...string) (*CommandHandler, *RedisACL) {
	routingPort, optionalArgsLine := r.getRoutingPortFromCommandLine(opt)
	args := []string{"-h", nodeIP, "acl", "list", optionalArgsLine}

	h := r.Provider.provideCommandHandler()
	h.buildCommand(args, routingPort, r.Auth.User)
	h.executeCommand()

	if h.Error != nil || strings.TrimSpace(h.ErrMsg) != "" || IsError(strings.TrimSpace(h.ExecMsg)) {
		h.Error = errors.Errorf("Failed to execute ACL LIST (%s): %s | %s | %v", nodeIP, h.ExecMsg, h.ErrMsg, h.Error)
		return h, nil
	}
	acl, err := NewRedisACL(h.ExecMsg)
	if err != nil {
		return h, nil
	}
	return h, acl
}
