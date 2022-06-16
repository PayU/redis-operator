package rediscli

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
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
	buildCommand(routingPort string, args []string, auth *RedisAuth, opt ...string) ([]string, map[string]string)
	executeCommand(args []string, multipFactorForTimeout ...float64) (string, string, error)
	//executeCommandWithPipe(pipeArgs []string, args []string, multipFactorForTimeout ...float64) (string, string, error)
	executeCommandReshard(args []string, multipFactorForTimeout ...float64) (string, string, error)
	buildRedisInfoModel(stdoutInfo string) (*RedisInfo, error)
	buildRedisClusterInfoModel(stdoutInfo string) (*RedisClusterInfo, error)
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

func (h *RunTimeCommandHandler) buildRedisInfoModel(stdoutInfo string) (*RedisInfo, error) {
	return NewRedisInfo(stdoutInfo)
}

func (h *RunTimeCommandHandler) buildRedisClusterInfoModel(stdoutInfo string) (*RedisClusterInfo, error) {
	return NewRedisClusterInfo(stdoutInfo)
}

func (h *RunTimeCommandHandler) buildCommand(routingPort string, args []string, auth *RedisAuth, opt ...string) ([]string, map[string]string) {

	routingPort, opt = routingPortDecider(routingPort, opt)
	if len(opt) > 0 {
		opt = trimCommandSpaces(opt)
		args = append(args, opt...)
	}
	if auth != nil {
		args = append([]string{"--user", auth.User}, args...)
	}
	args = append([]string{"-p", routingPort}, args...)

	return args, argListToArgMap(args)
}

/* Executes command and returns cmd stdout, stderr and runtime error if appears
 *  args: arguments, flags and their values, in the order they should appear as if they were executed in the cli itself
 */
func (h *RunTimeCommandHandler) executeCommandReshard(args []string, multipFactorForTimeout ...float64) (string, string, error) {

	var stdout, stderr bytes.Buffer

	multipFactor := 1.0
	if len(multipFactorForTimeout) > 0 {
		multipFactor = multipFactorForTimeout[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(multipFactor)*defaultRedisCliTimeout)
	defer cancel()

	argLine := ""
	// for _, arg := range pipeArgs {
		// argLine += arg + " "
	// }
	// if len(pipeArgs) > 0 {
		// argLine += "| "
	// }
	argLine += ""
	for _, arg := range args {
		argLine += " " + arg
	}

	println("redis-cli" + argLine)

	cmd := exec.CommandContext(ctx, "redis-cli", argLine)
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

func (h *RunTimeCommandHandler) executeCommand(args []string, multipFactorForTimeout ...float64) (string, string, error) {

	var stdout, stderr bytes.Buffer

	multipFactor := 1.0
	if len(multipFactorForTimeout) > 0 {
		multipFactor = multipFactorForTimeout[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(multipFactor)*defaultRedisCliTimeout)
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

/* Executes command and returns cmd stdout, stderr and runtime error if appears
 *  pipeArgs: argument list which will start the pipe the args list will be added to
 *  args: arguments, flags and their values, in the order they should appear as if they were executed in the cli itself
 */
// func (h *RunTimeCommandHandler) executeCommandWithPipe(pipeArgs []string, args []string, multipFactorForTimeout ...float64) (string, string, error) {
// 
	// var stdout, stderr bytes.Buffer
// 
	// multipFactor := 1.0
	// if len(multipFactorForTimeout) > 0 {
		// multipFactor = multipFactorForTimeout[0]
	// }
// 
	// ctx, cancel := context.WithTimeout(context.Background(), time.Duration(multipFactor)*defaultRedisCliTimeout)
	// defer cancel()
// 
	// argLine := ""
	// for _, arg := range pipeArgs {
		// argLine += arg + " "
	// }
	// if len(pipeArgs) > 0 {
		// argLine += "| "
	// }
	// argLine += "redis-cli"
	// for _, arg := range args {
		// argLine += " " + arg
	// }
// 
	// cmd := exec.CommandContext(ctx, "bash", "-c", argLine)
// 
	// cmd.Stdout = &stdout
	// cmd.Stderr = &stderr
// 
	// if err := cmd.Start(); err != nil {
		// return stdout.String(), stderr.String(), err
	// }
// 
	// if err := cmd.Wait(); err != nil {
		// if e, ok := err.(*exec.ExitError); ok {
// 
			// // If the process exited by itself, just return the error to the caller
			// if e.Exited() {
				// return stdout.String(), stderr.String(), e
			// }
// 
			// // We know now that the process could be started, but didn't exit
			// // by itself. Something must have killed it. If the context is done,
			// // we can *assume* that it has been killed by the exec.Command.
			// // Let's return ctx.Err() so our user knows that this *might* be
			// // the case.
// 
			// select {
			// case <-ctx.Done():
				// return stdout.String(), stderr.String(), errors.Errorf("exec of %v failed with: %v", args, ctx.Err())
			// default:
				// return stdout.String(), stderr.String(), errors.Errorf("exec of %v failed with: %v", args, e)
			// }
		// }
		// return stdout.String(), stderr.String(), err
	// }
// 
	// stdOutput := strings.TrimSpace(stdout.String())
	// errOutput := strings.TrimSpace(stderr.String())
// 
	// if errOutput != "" {
		// return stdOutput, errOutput, errors.New(errOutput)
	// }
	// if stdOutput != "" && strings.Contains(strings.ToLower(stdOutput), "error:") {
		// return stdOutput, stdOutput, errors.New(stdOutput)
	// }
	// return stdOutput, errOutput, nil
//}

// Helpers

func argLineToArgMap(argLine string, argMap map[string]string) {
	flagFormat := "[-]+[\\w\\d\\-\\.\\:]+"
	argFormat := "[^-][\\w\\d\\.\\:|-]+[^-]+"
	argLineFormat := flagFormat + "\\s+" + argFormat + "|" + flagFormat
	comp, _ := regexp.Compile(argLineFormat)
	matchingSubstrings := comp.FindAllStringSubmatch(argLine, -1)
	if len(matchingSubstrings) > 0 {
		for _, match := range matchingSubstrings {
			if len(match) > 0 {
				m := match[0]
				c, _ := regexp.Compile(flagFormat)
				flag := strings.TrimSpace(c.FindStringSubmatch(m)[0])
				arg := strings.TrimSpace(strings.Replace(m, flag, "", -1))
				argMap[flag] = arg
			}
		}
	}
}

func argListToArgMap(argList []string) map[string]string {
	argMap := make(map[string]string)
	argLine := ""
	for _, arg := range argList {
		argLine += arg + " "
	}
	argLineToArgMap(argLine, argMap)
	return argMap
}

func trimCommandSpaces(command []string) []string {
	if len(command) > 0 {
		var trimmedCommand []string
		for _, arg := range command {
			trimmed := strings.TrimSpace(arg)
			if len(trimmed) > 0 {
				trimmedCommand = append(trimmedCommand, trimmed)
			}
		}
		return trimmedCommand
	}
	return command
}

func routingPortDecider(cliDefaultPort string, opt []string) (string, []string) {
	port := cliDefaultPort
	if len(opt) > 0 {
		var optionalArgs []string
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
				optionalArgs = append(optionalArgs, trimmed)
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
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute cluster create (%v): %s | %s | %v", fullAddresses, stdout, stderr, err)
	}
	return stdout, nil
}

func (r *RedisCLI) ClusterCheck(nodeAddr string, opt ...string) (string, error) {
	args := []string{"--cluster", "check", addressPortDecider(nodeAddr, r.Port)}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
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
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute cluster add node (%s, %s, %s): %s | %s | %v", newNodeAddr, existingNodeAddr, leaderID, stdout, stderr, err)
	}
	return stdout, nil
}

// AddLeader uses the '--cluster add-node' option on redis-cli to add a node to the cluster
// newNodeAddr: Address of the follower that will join the cluster in a format of <ip>:<port> or <ip>: or <ip>
// existingNodeAddr: IP of a node in the cluster in a format of <ip>:<port> or <ip>: or <ip>
// leaderID: 	Redis ID of the leader that the new follower will replicate
// In case port won't bw provided as part of the given addresses, cli default port will be added automatically to the address
func (r *RedisCLI) AddLeader(newNodeAddr string, existingNodeAddr string, opt ...string) (string, error) {
	args := []string{"--cluster", "add-node", addressPortDecider(newNodeAddr, r.Port), addressPortDecider(existingNodeAddr, r.Port)}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args, 2)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute cluster add node (%s, %s): %s | %s | %v", newNodeAddr, existingNodeAddr, stdout, stderr, err)
	}
	return stdout, nil
}

// DelNode uses the '--cluster del-node' option of redis-cli to remove a node from the cluster
// nodeIP: any node of the cluster
// nodeID: node that needs to be removed
func (r *RedisCLI) DelNode(nodeIP string, nodeID string, opt ...string) (string, error) {
	args := []string{"--cluster", "del-node", addressPortDecider(nodeIP, r.Port), nodeID}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || stderr != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute cluster del-node (%s, %s): %s | %s | %v", nodeIP, nodeID, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-info
func (r *RedisCLI) ClusterInfo(nodeIP string, opt ...string) (*RedisClusterInfo, string, error) {

	args := []string{"-h", nodeIP, "cluster", "info"}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return nil, "", errors.Errorf("Failed to execute CLUSTER INFO (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	c, e := r.Handler.buildRedisClusterInfoModel(stdout)
	return c, stdout, e
}

// https://redis.io/commands/info
func (r *RedisCLI) Info(nodeIP string, opt ...string) (*RedisInfo, string, error) {

	args := []string{"-h", nodeIP, "info"}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return nil, "", errors.Errorf("Failed to execute INFO (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	c, e := r.Handler.buildRedisInfoModel(stdout)
	return c, stdout, e
}

func (r *RedisCLI) DBSIZE(nodeIP string, opt ...string) (int64, string, error) {
	args := []string{"-h", nodeIP, "DBSIZE"}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return 0, stdout, errors.Errorf("Failed to execute INFO (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	dbsize, err := strconv.ParseInt(stdout, 10, 64)
	return dbsize, stdout, err
}

// https://redis.io/commands/ping
func (r *RedisCLI) Ping(nodeIP string, message ...string) (string, error) {
	args := []string{"-h", nodeIP, "ping"}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, message...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute INFO (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return strings.TrimSpace(stdout), nil
}

// https://redis.io/commands/cluster-nodes
func (r *RedisCLI) ClusterNodes(nodeIP string, opt ...string) (*RedisClusterNodes, string, error) {
	args := []string{"-h", nodeIP, "cluster", "nodes"}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return nil, "", errors.Errorf("Failed to execute CLUSTER NODES(%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return NewRedisClusterNodes(stdout), stdout, nil
}

// https://redis.io/commands/cluster-myid
func (r *RedisCLI) MyClusterID(nodeIP string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "myid"}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
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
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
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
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return nil, "", errors.Errorf("Failed to execute CLUSTER REPLICAS (%s, %s): %s | %s | %v", nodeIP, leaderNodeID, stdout, stderr, err)
	}
	return NewRedisClusterNodes(stdout), stdout, err
}

// https://redis.io/commands/cluster-failover
func (r *RedisCLI) ClusterFailover(nodeIP string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "failover"}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER FAILOVER (%s, %v): %s | %s | %v", nodeIP, opt, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-meet
func (r *RedisCLI) ClusterMeet(nodeIP string, newNodeIP string, newNodePort string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "meet", newNodeIP, newNodePort}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER MEET (%s, %s, %s, %v): %s | %s | %v", nodeIP, newNodeIP, newNodePort, opt, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-reset
func (r *RedisCLI) ClusterReset(nodeIP string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "reset"}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER RESET (%s, %v): %s | %s | %v", nodeIP, opt, stdout, stderr, err)
	}
	return stdout, nil
}

func (r *RedisCLI) ClusterRebalance(nodeIP string, useEmptyMasters bool, opt ...string) (bool, string, error) {
	args := []string{"--cluster", "rebalance", addressPortDecider(nodeIP, r.Port)}
	if useEmptyMasters {
		args = append(args, "--cluster-use-empty-masters")
	}
	args = append(args, "--cluster-yes")
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args, 50)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return false, stdout, errors.Errorf("Failed to execute cluster rebalance (%v): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return true, stdout, nil
}

func (r *RedisCLI) ClusterReshard(nodeIP string, sourceId string, targetId string, slots int, opt ...string) (bool, string, error) {
	args := []string{
		"--cluster reshard", addressPortDecider(nodeIP, r.Port),
		"--cluster-from", sourceId,
		"--cluster-to", targetId,
		"--cluster-slots", fmt.Sprint(slots),
		"--cluster-yes",
	}
	//args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommandReshard(args, 50)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return false, stdout, errors.Errorf("Failed to execute cluster reshard (%v): from [%s] to [%s] stdout: %s | stderr : %s | err: %v", nodeIP, sourceId, targetId, stdout, stderr, err)
	}
	return true, stdout, nil
}

// https://redis.io/commands/flushall
func (r *RedisCLI) Flushall(nodeIP string, opt ...string) (string, error) {

	args := []string{"-h", nodeIP, "flushall"}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute FLUSHALL (%s, %v): %s | %s | %v", nodeIP, opt, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/cluster-replicate
func (r *RedisCLI) ClusterReplicate(nodeIP string, leaderID string, opt ...string) (string, error) {
	args := []string{"-h", nodeIP, "cluster", "replicate", leaderID}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute CLUSTER REPLICATE (%s, %s): %s | %s | %v", nodeIP, leaderID, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/acl-load
func (r *RedisCLI) ACLLoad(nodeIP string, opt ...string) (string, error) {

	args := []string{"-h", nodeIP, "acl", "load"}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || strings.TrimSpace(stdout) != "OK" {
		return stdout, errors.Errorf("Failed to execute ACL LOAD (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return stdout, nil
}

// https://redis.io/commands/acl-list
func (r *RedisCLI) ACLList(nodeIP string, opt ...string) (*RedisACL, string, error) {

	args := []string{"-h", nodeIP, "acl", "list"}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
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

func (r *RedisCLI) ClusterFix(nodeIP string, opt ...string) (bool, string, error) {
	args := []string{"--cluster", "fix", addressPortDecider(nodeIP, r.Port), "--cluster-fix-with-unreachable-masters", "--cluster-yes"}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth, opt...)
	stdout, stderr, err := r.Handler.executeCommand(args, 50)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return false, stdout, errors.Errorf("Failed to execute cluster fix (%v): %s | %s | %v", addressPortDecider(nodeIP, r.Port), stdout, stderr, err)
	}
	return true, stdout, nil
}

func (r *RedisCLI) Role(nodeIP string) (string, error) {
	args := []string{"-h", nodeIP, "role"}
	args, _ = r.Handler.buildCommand(r.Port, args, r.Auth)
	stdout, stderr, err := r.Handler.executeCommand(args)
	if err != nil || strings.TrimSpace(stderr) != "" || IsError(strings.TrimSpace(stdout)) {
		return stdout, errors.Errorf("Failed to execute Role (%s): %s | %s | %v", nodeIP, stdout, stderr, err)
	}
	return stdout, nil
}
