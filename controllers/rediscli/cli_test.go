package rediscli

import (
	"fmt"
	"strings"
	"testing"
)

type TestCommandHandler struct{}

func (r *TestCommandHandler) buildRedisInfoModel(stdoutInfo string) (*RedisInfo, error) {
	return &RedisInfo{}, nil
}

func (h *TestCommandHandler) buildRedisClusterInfoModel(stdoutInfo string) (*RedisClusterInfo, error) {
	return &RedisClusterInfo{}, nil
}

func (h *TestCommandHandler) buildCommand(routingPort string, args []string, auth *RedisAuth, opt ...string) ([]string, map[string]string) {
	if auth != nil {
		args = append([]string{"--user", auth.User}, args...)
	}
	routingPort, opt = routingPortDecider(routingPort, opt)
	args = append([]string{"-p", routingPort}, args...)
	if len(opt) > 0 {
		args = append(args, opt...)
	}
	return args, argListToArgMap(args)
}

func (h *TestCommandHandler) executeCommand(args []string, multipFactorForTimeout ...float64) (string, string, error) {
	executedCommand := ""
	for _, arg := range args {
		executedCommand += arg + " "
	}
	return executedCommand, "", nil
}

func (h *TestCommandHandler) executeCommandWithPipe(pipeArgs []string, args []string, multipFactorForTimeout ...float64) (string, string, error) {
	executedCommand := ""
	for _, arg := range pipeArgs {
		executedCommand += arg + " "
	}
	executedCommand += "| redis-cli"
	for _, arg := range args {
		executedCommand += " " + arg
	}
	return executedCommand, "", nil
}

func mapToPrintableStr(argMap map[string]string) string {
	toStr := "{\n"
	for key, val := range argMap {
		toStr += "  " + key + " : " + val + "\n"
	}
	toStr += "}\n"
	return toStr
}

func resultHandler(expected string, result string, testCase string, argMap map[string]string, expectedArgMap map[string]string) {
	msg := "[CLI Unit test]\nExpected result : " + expected + "\nActual result   : " + result
	msg += "\nExpected arg mapping result:\n" + mapToPrintableStr(expectedArgMap) + "Arg mapping result:\n" + mapToPrintableStr(argMap)
	if strings.Compare(strings.TrimSpace(expected), strings.TrimSpace(result)) != 0 {
		msg += "Test case " + testCase + " failed" + "\n\n"
		t.Errorf(msg)
	} else {
		msg += "Test case " + testCase + " passed" + "\n\n"
		t.Logf(msg)
	}
}

var r *RedisCLI
var t *testing.T

func TestRedisCLI(test *testing.T) {
	auth := &RedisAuth{"test_user"}
	r = &RedisCLI{nil, auth, "6380", nil}
	r.Handler = &TestCommandHandler{}
	t = test

	testClusterCreate()
	testClusterCheck()
	testAddFollower()
	testAddLeader()
	testDelNode()
	testClusterInfo()
	testInfo()
	testPing()
	testClusterNodes()
	testMyClusterID()
	testClusterForget()
	testClusterReplicas()
	testClusterFailOver()
	testClusterMeet()
	testClusterReset()
	testClusterRebalance()
	testClusterReshard()
	testFlushAll()
	testClusterReplicate()
	testACLLoad()
	testACLList()
	testClusterFix()
}

func testClusterCreate() {
	// Test 1 : Routing port is not provided, Address ports are not provided, no optional arguments
	addresses := []string{"127.0.0.1", "128.1.1.2:", "129.2.2.3", "130.3.3.4:"}
	execClusterCreateTest("1", addresses)
	// Test 2 : Routing port is provided, Only part of the address ports are provided, no other optional arguments
	addresses = []string{"127.0.0.1:8080", "128.1.1.2:6379", "129.2.2.3", "130.3.3.4:"}
	execClusterCreateTest("2", addresses, "-p 6379")
	// Test 3 : Routing port is provided, Only part of the addres ports are provided, other optional arguments are provided
	addresses = []string{"127.0.0.1:8080", "128.1.1.2:6379", "129.2.2.3", "130.3.3.4:"}
	execClusterCreateTest("3", addresses, "-p 6375 -optArg1 optVal1 -optArg2 optVal2")
	// Test 4 : Routing port is provided, All of the address ports are provided, other optional arguments are provided (routing port is not the first optional arg among them)
	addresses = []string{"127.0.0.1:8090", "128.1.1.2:8090", "129.2.2.3:8090", "130.3.3.4:8090"}
	execClusterCreateTest("4", addresses, "-optArg1 optVal1 -p 6363 -optArg2 optVal2")
	// Test 5 : Routing port is provided, Address ports are not provided, optional arguments are given as a parametrized list
	addresses = []string{"127.0.0.1:", "128.1.1.2:", "129.2.2.3", "130.3.3.4:"}
	execClusterCreateTest("5", addresses, "-optArg1 optVal1", "-p 6565", "-optArg2 optVal2")
}

func testClusterCheck() {
	// Test 1 : Routing port is not provided, address port is not provided, no optional arguments
	address := "127.0.0.1"
	execClusterCheckTest("1", address)
	// Test 2 : Routing port is provided, address port is not provided, no optional arguments
	address = "127.0.0.1:"
	execClusterCheckTest("2", address, "-p 6378")
	// Test 3 : Routing port is not provided, address port is provided, optional arguments provided
	address = "127.0.0.1:6397"
	execClusterCheckTest("3", address, "-optArg1 optVal1 -optArg2 optVal2")
	// Test 4 : Routing port is provided, address port is provided, optional arguments provided
	address = "127.0.0.1:6398"
	execClusterCheckTest("4", address, "-p 6398 -optArg1 optVal1")
	// Test 5 : Routing port is provided, Address port is not provided, optional arguments are given as a parametrized list
	address = "127.0.0.1:"
	execClusterCheckTest("5", address, "-optArg1 optVal1", "-p 8080")

}

func testAddFollower() {
	// Test 1 : Routing port is not provided, newNodeAddr port is not provided, existingNodeAddr port is not provided, no optional args
	newNodeAddr := "127.0.0.1"
	existingNodeAddr := "128.1.1.2:"
	leaderID := "abcdefg123456"
	execAddFollowerTest("1", newNodeAddr, existingNodeAddr, leaderID)
	// Test 2 : Routing port is provided, newNodeAddr port is provided, existingNodeAddr port is not provided, no optional args
	newNodeAddr = "127.0.0.1:6565"
	execAddFollowerTest("2", newNodeAddr, existingNodeAddr, leaderID, "-p 8080")
	// Test 3 : Routing port is not provided, newNodeAddr port is not provided, existingNodeAddr port is provided, optional arguments provided
	newNodeAddr = "127.0.0.1:"
	existingNodeAddr = "128.1.1.2:6377"
	execAddFollowerTest("3", newNodeAddr, existingNodeAddr, leaderID, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, newNodeAddr port is provided, existingNodeAddr port is provided, optional arguments provided
	newNodeAddr = "127.0.0.1:6377"
	execAddFollowerTest("4", newNodeAddr, existingNodeAddr, leaderID, "-p 6379 -optArg1 optVal1 -optArg2 optVal2")
	// Test 5 : Routing port is provided, newNodeAddr port is provided, existingNodeAddr port is provided, optional arguments provided as a parametrized arg list
	execAddFollowerTest("5", newNodeAddr, existingNodeAddr, leaderID, "-optArg1 optVal1", "-p 6379", "-optArg2 optVal2")
}

func testAddLeader() {
	// Test 1 : Routing port is not provided, newNodeAddr port is not provided, existingNodeAddr port is not provided, no optional args
	newNodeAddr := "127.0.0.1"
	existingNodeAddr := "128.1.1.2:"
	execAddLeaderTest("1", newNodeAddr, existingNodeAddr)
	// Test 2 : Routing port is provided, newNodeAddr port is provided, existingNodeAddr port is not provided, no optional args
	newNodeAddr = "127.0.0.1:6565"
	execAddLeaderTest("2", newNodeAddr, existingNodeAddr, "-p 8080")
	// Test 3 : Routing port is not provided, newNodeAddr port is not provided, existingNodeAddr port is provided, optional arguments provided
	newNodeAddr = "127.0.0.1:"
	existingNodeAddr = "128.1.1.2:6377"
	execAddLeaderTest("3", newNodeAddr, existingNodeAddr, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, newNodeAddr port is provided, existingNodeAddr port is provided, optional arguments provided
	newNodeAddr = "127.0.0.1:6377"
	execAddLeaderTest("4", newNodeAddr, existingNodeAddr, "-p 6379 -optArg1 optVal1 -optArg2 optVal2")
	// Test 5 : Routing port is provided, newNodeAddr port is provided, existingNodeAddr port is provided, optional arguments provided as a parametrized arg list
	execAddLeaderTest("5", newNodeAddr, existingNodeAddr, "-optArg1 optVal1", "-p 6379", "-optArg2 optVal2")
}

func testDelNode() {
	nodeIP := "127.0.0.1"
	nodeID := "abcde12345"
	// Test 1 : Routing port is not provided, optional arguments not provided
	execDelNodeTest("1", nodeIP, nodeID)
	// Test 2 : Routing port is provided, optional arguments not provided
	execDelNodeTest("2", nodeIP, nodeID, "-p 6399")
	// Test 3 : Routing port is provided, optional arguments provided
	execDelNodeTest("3", nodeIP, nodeID, "-p 6388 -optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments provided as parametrized arg list
	execDelNodeTest("4", nodeIP, nodeID, "-p 6398", "-optArg1 optVal1")
	// Test 5 : Routing port is not provided, optional arguments provided as parametrized arg list
	execDelNodeTest("5", nodeIP, nodeID, "-optArg1 optVal1", "-optArg2 optVal2")
}

func testClusterInfo() {
	nodeIP := "127.0.0.1"
	// Test 1 : Routing port is not provided, no optional arguments
	execClusterInfoTest("1", nodeIP)
	// Test 2 : Routing port is provided, no optional arguments
	execClusterInfoTest("2", nodeIP, "-p 6382")
	// Test 3 : Routing port is not provided, optional arguments provided
	execClusterInfoTest("3", nodeIP, "-optArg1 optVal1 -optArg2 optVal2")
	// Test 4 : Routing port is provided, optional arguments provided
	execClusterInfoTest("4", nodeIP, "-p 8080 -optArg1 optVal1")
	// Test 5 : Routing port is provided, optional arguments provided as parametrized arg list
	execClusterInfoTest("5", nodeIP, "-p 8090", "-optArg1 optVal1", "-optArg2 optVal2")
}

func testInfo() {
	nodeIP := "127.1.1.2"
	// Test 1 : Routing port is not provided, no optional arguments
	execInfoTest("1", nodeIP)
	// Test 2 : Routing port is provided, no optional arguments
	execInfoTest("2", nodeIP, "-p 8080")
	// Test 3 : Routing port is not provided, optional arguments provided
	execInfoTest("3", nodeIP, "-optArg1 optVal1 -optArg2 optVal2")
	// Test 4 : Routing port is provided, optional arguments are provided
	execInfoTest("4", nodeIP, "-optArg1 optVal1 -p 8080 -optArg2 optVal2")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execInfoTest("5", nodeIP, "-p 8080", "-optArg1 optVal1")
}

func testPing() {
	nodeIP := "127.0.0.1"
	// Test 1 : Routing port is not provided, no optional arguments
	execPingTest("1", nodeIP)
	// Test 2 : Routing port is provided, no optional arguments
	execPingTest("2", nodeIP, "-p 6379")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execPingTest("3", nodeIP, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments are provided
	execPingTest("4", nodeIP, "-optArg1 optVal1 -p 6388")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execPingTest("5", nodeIP, "-optArg1 optVal1 -p 6377 -optArg2 optVal2")
}

func testClusterNodes() {
	nodeIP := "127.0.0.1"
	// Test 1 : Routing port is not provided, no optional arguments
	execClusterNodesTest("1", nodeIP)
	// Test 2 : Routing port is provided, no optional arguments
	execClusterNodesTest("2", nodeIP, "-p 6377 -optArg1 optVal1")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execClusterNodesTest("3", nodeIP, "-optArg1 optVal1 -optArg2 optVal2")
	// Test 4 : Routing port is provided, optional arguments are provided
	execClusterNodesTest("4", nodeIP, "-optArg1 optVal1 -p 6388 -optArg2 optVal2")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execClusterNodesTest("5", nodeIP, "-p 6399", "-optArg1 optVal1")
}

func testMyClusterID() {
	nodeIP := "128.1.0.1"
	// Test 1 : Routing port is not provided, no optional arguments
	execMyClusterIDTest("1", nodeIP)
	// Test 2 : Routing port is provided, no optional arguments
	execMyClusterIDTest("2", nodeIP, "-p 6379")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execMyClusterIDTest("3", nodeIP, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments are provided
	execMyClusterIDTest("4", nodeIP, "-optArg1 optVal1 -p 6388")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execMyClusterIDTest("5", nodeIP, "-p 6399", "-optArg1 optVal1")
}

func testClusterForget() {
	nodeIP := "127.8.1.0"
	forgetNodeID := "abcd1234"
	// Test 1 : Routing port is not provided, no optional arguments
	execClusterForgetTest("1", nodeIP, forgetNodeID)
	// Test 2 : Routing port is provided, no optional arguments
	execClusterForgetTest("2", nodeIP, forgetNodeID, "-p 6388")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execClusterForgetTest("3", nodeIP, forgetNodeID, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments are provided
	execClusterForgetTest("4", nodeIP, forgetNodeID, "-p 6399 -optArg1 optVal1")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execClusterForgetTest("5", nodeIP, forgetNodeID, "-p 6489", "-optArg1 optVal1")
}

func testClusterReplicas() {
	nodeIP := "127.0.0.1"
	leaderNodeID := "abcd1234"
	// Test 1 : Routing port is not provided, no optional arguments
	execClusterReplicasTest("1", nodeIP, leaderNodeID)
	// Test 2 : Routing port is provided, no optional arguments
	execClusterReplicasTest("2", nodeIP, leaderNodeID, "-p 6377")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execClusterReplicasTest("3", nodeIP, leaderNodeID, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments are provided
	execClusterReplicasTest("4", nodeIP, leaderNodeID, "-p 6388 -optArg1 optVal1")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execClusterReplicasTest("5", nodeIP, leaderNodeID, "-p 6388", "-optArg1 optVal1")
}

func testClusterFailOver() {
	nodeIP := "127.0.0.1"
	// Test 1 : Routing port is not provided, no optional arguments
	execClusterFailOverTest("1", nodeIP)
	// Test 2 : Routing port is provided, no optional arguments
	execClusterFailOverTest("2", nodeIP, "-p 6377")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execClusterFailOverTest("3", nodeIP, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments are provided
	execClusterFailOverTest("4", nodeIP, "-p 6399 -optArg1 optVal1")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execClusterFailOverTest("5", nodeIP, "-p 6399", "-optArg1 optVal1")
}

func testClusterMeet() {
	nodeIP := "127.0.0.1"
	newNodeIP := "128.0.1.2"
	newNodePort := "6397"
	// Test 1 : Routing port is not provided, no optional arguments
	execClusterMeetTest("1", nodeIP, newNodeIP, newNodePort)
	// Test 2 : Routing port is provided, no optional arguments
	execClusterMeetTest("2", nodeIP, newNodeIP, newNodePort, "-p 6388")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execClusterMeetTest("3", nodeIP, newNodeIP, newNodePort, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments are provided
	execClusterMeetTest("4", nodeIP, newNodeIP, newNodePort, "-p 8389 -optArg1 optVal1")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execClusterMeetTest("5", nodeIP, newNodeIP, newNodePort, "-p 8389", "-optArg1 optVal1")
}

func testClusterReset() {
	nodeIP := "127.0.0.1"
	// Test 1 : Routing port is not provided, no optional arguments
	execClusterResetTest("1", nodeIP)
	// Test 2 : Routing port is provided, no optional arguments
	execClusterResetTest("2", nodeIP, "-p 8383")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execClusterResetTest("3", nodeIP, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments are provided
	execClusterResetTest("4", nodeIP, "-p 8384 -optArg1 optVal1")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execClusterResetTest("5", nodeIP, "-p 6399", "-optArg1 optVal1")
}

func testClusterRebalance() {
	nodeIP := "127.0.0.1"
	// Test 1 : Routing port is not provided, no optional arguments
	execClusterRebalanceTest("1", nodeIP, true)
	// Test 2 : Routing port is provided, no optional arguments
	execClusterRebalanceTest("2", nodeIP, false, "-p 8383")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execClusterRebalanceTest("3", nodeIP, true, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments are provided
	execClusterRebalanceTest("4", nodeIP, false, "-p 8384 -optArg1 optVal1")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execClusterRebalanceTest("5", nodeIP, true, "-p 6399", "-optArg1 optVal1")
}

func testClusterReshard() {
	nodeIP := "127.0.0.1"
	// Test 1 : Routing port is not provided, no optional arguments
	execClusterReshardTest("1", nodeIP, "abc", "edf", 16384)
	// Test 2 : Routing port is provided, no optional arguments
	execClusterReshardTest("2", nodeIP, "abc", "edf", 16384, "-p 8383")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execClusterReshardTest("3", nodeIP, "abc", "edf", 16384, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments are provided
	execClusterReshardTest("4", nodeIP, "abc", "edf", 16384, "-p 8384 -optArg1 optVal1")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execClusterReshardTest("5", nodeIP, "abc", "edf", 16384, "-p 6399", "-optArg1 optVal1")
}

func testFlushAll() {
	nodeIP := "128.0.1.1"
	// Test 1 : Routing port is not provided, no optional arguments
	execFlushAllTest("1", nodeIP)
	// Test 2 : Routing port is provided, no optional arguments
	execFlushAllTest("2", nodeIP, "-p 6373")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execFlushAllTest("3", nodeIP, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments are provided
	execFlushAllTest("4", nodeIP, "-p 8363 -optArg1 optVal1")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execFlushAllTest("5", nodeIP, "-p 8363", "-optArg1 optVal1")
}

func testClusterReplicate() {
	nodeIP := "127.0.0.1"
	leaderID := "abcdefg123456"
	// Test 1 : Routing port is not provided, no optional arguments
	execClusterReplicateTest("1", nodeIP, leaderID)
	// Test 2 : Routing port is provided, no optional arguments
	execClusterReplicateTest("2", nodeIP, leaderID, "-p 8090")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execClusterReplicateTest("3", nodeIP, leaderID, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments are provided
	execClusterReplicateTest("4", nodeIP, leaderID, "-optArg1 optVal1")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execClusterReplicateTest("5", nodeIP, leaderID, "-p 9080 -optArg1 optVal1")
}

func testACLLoad() {
	nodeIP := "129.3.6.1"
	// Test 1 : Routing port is not provided, no optional arguments
	execACLLoadTest("1", nodeIP)
	// Test 2 : Routing port is provided, no optional arguments
	execACLLoadTest("2", nodeIP, "-p 8080")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execACLLoadTest("3", nodeIP, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments are provided
	execACLLoadTest("4", nodeIP, "-p 9090 -optArg1 optVal1")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execACLLoadTest("5", nodeIP, "-p 9090", "-optArg1 optVal1")
}

func testACLList() {
	nodeIP := "129.4.6.2"
	// Test 1 : Routing port is not provided, no optional arguments
	execACLListTest("1", nodeIP)
	// Test 2 : Routing port is provided, no optional arguments
	execACLListTest("2", nodeIP, "-p 6379")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execACLListTest("3", nodeIP, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments are provided
	execACLListTest("4", nodeIP, "-p 6381 -optArg1 optVal1")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execACLListTest("5", nodeIP, "-p 6381", "-optArg1 optVal1")
}

func testClusterFix() {
	nodeIP := "129.4.6.2"
	// Test 1 : Routing port is not provided, no optional arguments
	execClusterFixTest("1", nodeIP)
	// Test 2 : Routing port is provided, no optional arguments
	execClusterFixTest("2", nodeIP, "-p 6379")
	// Test 3 : Routing port is not provided, optional arguments are provided
	execClusterFixTest("3", nodeIP, "-optArg1 optVal1")
	// Test 4 : Routing port is provided, optional arguments are provided
	execClusterFixTest("4", nodeIP, "-p 6381 -optArg1 optVal1")
	// Test 5 : Routing port is provided, optional arguments are provided as parametrized arg list
	execClusterFixTest("5", nodeIP, "-p 6381", "-optArg1 optVal1")
}

// Test exec helpers

func execClusterCreateTest(testCaseId string, addresses []string, opt ...string) {
	result, _ := r.ClusterCreate(addresses, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	updatedAddresses := addressesPortDecider(addresses, r.Port)
	expectedArgList := append([]string{"--cluster", "create"}, updatedAddresses...)
	expectedArgList = append(expectedArgList, "--cluster-yes")
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster Create "+testCaseId, argMap, expectedArgMap)
}

func execClusterCheckTest(testCaseId string, address string, opt ...string) {
	result, _ := r.ClusterCheck(address, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"--cluster", "check", addressPortDecider(address, r.Port)}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster Check "+testCaseId, argMap, expectedArgMap)
}

func execAddFollowerTest(testCaseId string, newNodeAddr string, existingNodeAddr string, leaderID string, opt ...string) {
	result, _ := r.AddFollower(newNodeAddr, existingNodeAddr, leaderID, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	newNodeAddr = addressPortDecider(newNodeAddr, r.Port)
	existingNodeAddr = addressPortDecider(existingNodeAddr, r.Port)
	leadershipType := "--cluster-slave"
	leaderIdFlag := "--cluster-master-id"
	expectedArgList := []string{"--cluster", "add-node"}
	expectedArgList = append(expectedArgList, newNodeAddr, existingNodeAddr, leadershipType, leaderIdFlag, leaderID)
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Add follower "+testCaseId, argMap, expectedArgMap)
}

func execAddLeaderTest(testCaseId string, newNodeAddr string, existingNodeAddr string, opt ...string) {
	result, _ := r.AddLeader(newNodeAddr, existingNodeAddr, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"--cluster", "add-node", addressPortDecider(newNodeAddr, r.Port), addressPortDecider(existingNodeAddr, r.Port)}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Add Leader "+testCaseId, argMap, expectedArgMap)
}

func execDelNodeTest(testCaseId string, nodeIP string, nodeID string, opt ...string) {
	result, _ := r.DelNode(nodeIP, nodeID, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"--cluster", "del-node", addressPortDecider(nodeIP, r.Port), nodeID}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Delete Node "+testCaseId, argMap, expectedArgMap)
}

func execClusterInfoTest(testCaseId string, nodeIP string, opt ...string) {
	_, result, _ := r.ClusterInfo(nodeIP, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"-h", nodeIP, "cluster", "info"}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster Info "+testCaseId, argMap, expectedArgMap)
}

func execInfoTest(testCaseId string, nodeIP string, opt ...string) {
	_, result, _ := r.Info(nodeIP, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"-h", nodeIP, "info"}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Info "+testCaseId, argMap, expectedArgMap)
}

func execPingTest(testCaseId string, nodeIP string, opt ...string) {
	result, _ := r.Ping(nodeIP, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"-h", nodeIP, "ping"}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Ping "+testCaseId, argMap, expectedArgMap)
}

func execClusterNodesTest(testCaseId string, nodeIP string, opt ...string) {
	_, result, _ := r.ClusterNodes(nodeIP, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"-h", nodeIP, "cluster", "nodes"}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster Nodes "+testCaseId, argMap, expectedArgMap)
}

func execMyClusterIDTest(testCaseId string, nodeIP string, opt ...string) {
	result, _ := r.MyClusterID(nodeIP, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgLine := []string{"-h", nodeIP, "cluster", "myid"}
	expectedArgLine, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgLine, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgLine)
	resultHandler(expectedResult, result, "My Cluster ID "+testCaseId, argMap, expectedArgMap)
}

func execClusterForgetTest(testCaseId string, nodeIP string, forgetNodeID string, opt ...string) {
	result, _ := r.ClusterForget(nodeIP, forgetNodeID, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"-h", nodeIP, "cluster", "forget", forgetNodeID}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster Forget "+testCaseId, argMap, expectedArgMap)
}

func execClusterReplicasTest(testCaseId string, nodeIP string, leaderNodeID string, opt ...string) {
	_, result, _ := r.ClusterReplicas(nodeIP, leaderNodeID, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"-h", nodeIP, "cluster", "replicas", leaderNodeID}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster Replicas "+testCaseId, argMap, expectedArgMap)
}

func execClusterFailOverTest(testCaseId string, nodeIP string, opt ...string) {
	result, _ := r.ClusterFailover(nodeIP, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"-h", nodeIP, "cluster", "failover"}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster Failover "+testCaseId, argMap, expectedArgMap)
}

func execClusterMeetTest(testCaseId string, nodeIP string, newNodeIP string, newNodePort string, opt ...string) {
	result, _ := r.ClusterMeet(nodeIP, newNodeIP, newNodePort, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"-h", nodeIP, "cluster", "meet", newNodeIP, newNodePort}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster Meet "+testCaseId, argMap, expectedArgMap)
}

func execClusterResetTest(testCaseId string, nodeIP string, opt ...string) {
	result, _ := r.ClusterReset(nodeIP, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"-h", nodeIP, "cluster", "reset"}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster Reset "+testCaseId, argMap, expectedArgMap)
}

func execClusterRebalanceTest(testCaseId string, nodeIP string, useEmptyMasters bool, opt ...string) {
	_, result, _ := r.ClusterRebalance(nodeIP, useEmptyMasters, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(fmt.Sprint(result), argMap)
	expectedArgList := []string{"--cluster", "rebalance", addressPortDecider(nodeIP, r.Port)}
	if useEmptyMasters {
		expectedArgList = append(expectedArgList, "--cluster-use-empty-masters")
	}
	expectedArgList = append(expectedArgList, "--cluster-yes")
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, fmt.Sprint(result), "Cluster Rebalance "+testCaseId, argMap, expectedArgMap)
}

func execClusterReshardTest(testCaseId string, nodeIP string, sourceId string, targetId string, slots int, opt ...string) {
	_, result, _ := r.ClusterReshard(nodeIP, sourceId, targetId, slots, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(fmt.Sprint(result), argMap)
	expectedArgList := []string{"--cluster reshard", addressPortDecider(nodeIP, r.Port)}
	expectedArgList = append(expectedArgList, "--cluster-from", sourceId, "--cluster-to", targetId)
	expectedArgList = append(expectedArgList, "--cluster-slots", fmt.Sprint(slots), "--cluster-yes")
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, fmt.Sprint(result), "Cluster Reshard "+testCaseId, argMap, expectedArgMap)
}

func execFlushAllTest(testCaseId string, nodeIP string, opt ...string) {
	result, _ := r.Flushall(nodeIP, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"-h", nodeIP, "flushall"}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Flush All "+testCaseId, argMap, expectedArgMap)
}

func execClusterReplicateTest(testCaseId string, nodeIP string, leaderID string, opt ...string) {
	result, _ := r.ClusterReplicate(nodeIP, leaderID, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"-h", nodeIP, "cluster", "replicate", leaderID}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster replicate "+testCaseId, argMap, expectedArgMap)
}

func execACLLoadTest(testcaseId string, nodeIP string, opt ...string) {
	result, _ := r.ACLLoad(nodeIP, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"-h", nodeIP, "acl", "load"}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "ACLLoad "+testcaseId, argMap, expectedArgMap)
}

func execACLListTest(testCaseId string, nodeIP string, opt ...string) {
	_, result, _ := r.ACLList(nodeIP, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"-h", nodeIP, "acl", "list"}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "ACLList "+testCaseId, argMap, expectedArgMap)
}

func execClusterFixTest(testCaseId string, nodeIP string, opt ...string) {
	_, result, _ := r.ClusterFix(nodeIP, opt...)
	argMap := make(map[string]string)
	argLineToArgMap(result, argMap)
	expectedArgList := []string{"--cluster", "fix", addressPortDecider(nodeIP, r.Port), "--cluster-fix-with-unreachable-masters", "--cluster-yes"}
	expectedArgList, expectedArgMap := r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	pipeArgs := []string{"yes", "yes"}
	expectedResult, _, _ := r.Handler.executeCommandWithPipe(pipeArgs, expectedArgList)
	resultHandler(expectedResult, result, "Cluster fix "+testCaseId, argMap, expectedArgMap)
}
