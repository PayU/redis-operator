package rediscli

import (
	"fmt"
	"strings"
	"testing"
)

type TestCommandHandler struct{}

func (h *TestCommandHandler) buildCommand(routingPort string, args []string, auth *RedisAuth, opt ...string) []string {
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

func (h *TestCommandHandler) executeCommand(args []string) (string, string, error) {
	executedCommand := ""
	for _, arg := range args {
		executedCommand += arg + " "
	}
	return executedCommand, "", nil
}

func resultHandler(expected string, result string, testCase string) {
	if strings.Compare(strings.TrimSpace(expected), strings.TrimSpace(result)) != 0 {
		t.Fatalf("[CLI Unit test]\nExpected result : %v\nActual result   : %v\nTest case %v failed", expected, result, testCase)
	} else {
		fmt.Printf("[CLI Unit test]\nExpected result : %v\nActual result   : %v\nTest case %v passed\n", expected, result, testCase)
	}
	fmt.Println()
}

var r *RedisCLI
var t *testing.T

func TestRedisCLI(test *testing.T) {
	r = &RedisCLI{nil, nil, "6380", nil}
	r.Handler = &TestCommandHandler{}
	t = test

	testClusterCreate()
	testClusterCheck()
	testAddFollower()
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
	testFlushAll()
	testClusterReplicate()
	testACLLoad()
	testACLList()

	t.Fatal("For test")
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

	// Test 4 : Routing port is provided, All of the address ports are provided, other opional arguments are provided (routing port is not the first optional arg among them)
	addresses = []string{"127.0.0.1:8090", "128.1.1.2:8090", "129.2.2.3:8090", "130.3.3.4:8090"}
	execClusterCreateTest("4", addresses, "-optArg1 optVal1 -p 6363 -optArg2 optVal2")

	// Test 5 : Routing port is provided, Address ports are not provided, optional arguments are given as a parametrized list
	addresses = []string{"127.0.0.1:", "128.1.1.2:", "129.2.2.3", "130.3.3.4:"}
	execClusterCreateTest("5", addresses, "-optArg1 optVal1", "-p 6565", "-optArg2 optVal2")

}

func testClusterCheck() {
	// Test 1 : Routing port is not provided, adress port is not provided, no optional arguments
	address := "127.0.0.1"
	execClusterCheckTest("1", address)

	// Test 2 : Routing port is provided, address port is not provided, no optional arguments
	address = "127.0.0.1:"
	execClusterCheckTest("2", address, "-p 6378")

	// Test 3 : Routing port is not provided, address port is provided, optional arguments provided
	address = "127.0.0.1:6397"
	execClusterCheckTest("3", address, "-optArg1 optVal1 -optArg2 optVal2")

	// Test 4 : Routing port is provided, adress port is provided, optional arguments provided
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

	// Test 5 : Routing port is proivded, optional arguments are provided as parametrized arg list
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
	// Test 5 : Routing port is proivded, optional arguments are provided as parametrized arg list
	execClusterForgetTest("5", nodeIP, forgetNodeID, "-p 6489", "-optArg1 optVal1")
}

func testClusterReplicas() {}

func testClusterFailOver() {}

func testClusterMeet() {}

func testClusterReset() {}

func testFlushAll() {}

func testClusterReplicate() {}

func testACLLoad() {}

func testACLList() {}

func execClusterReplicasTest(testCaseId string, nodeIP string, leaderNodeID string, opt ...string) {}

func execClusterFailOverTest(testCaseId string, nodeIP string, opt ...string) {}

//func execClusterMeet(testCaseId string) {}

// Test exec helpers

func execClusterCreateTest(testCaseId string, addresses []string, opt ...string) {
	result, _ := r.ClusterCreate(addresses, opt...)
	updatedAddresses := addressesPortDecider(addresses, r.Port)
	expectedArgList := append([]string{"--cluster", "create"}, updatedAddresses...)
	expectedArgList = append(expectedArgList, "--cluster-yes")
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster Create "+testCaseId)
}

func execClusterCheckTest(testCaseId string, address string, opt ...string) {
	result, _ := r.ClusterCheck(address, opt...)
	expectedArgList := []string{"--cluster", "check", addressPortDecider(address, r.Port)}
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster Check "+testCaseId)
}

func execAddFollowerTest(testCaseId string, newNodeAddr string, existingNodeAddr string, leaderID string, opt ...string) {
	result, _ := r.AddFollower(newNodeAddr, existingNodeAddr, leaderID, opt...)
	expectedArgList := []string{"--cluster", "add-node", addressPortDecider(newNodeAddr, r.Port), addressPortDecider(existingNodeAddr, r.Port), "--cluster-slave", "--cluster-master-id", leaderID}
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Add follower "+testCaseId)
}

func execDelNodeTest(testCaseId string, nodeIP string, nodeID string, opt ...string) {
	result, _ := r.DelNode(nodeIP, nodeID, opt...)
	expectedArgList := []string{"--cluster", "del-node", addressPortDecider(nodeIP, r.Port), nodeID}
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Delete Node "+testCaseId)
}

func execClusterInfoTest(testCaseId string, nodeIP string, opt ...string) {
	_, result, _ := r.ClusterInfo(nodeIP, opt...)
	expectedArgList := []string{"-h", nodeIP, "cluster", "info"}
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster Info "+testCaseId)
}

func execInfoTest(testCaseId string, nodeIP string, opt ...string) {
	_, result, _ := r.Info(nodeIP, opt...)
	expectedArgList := []string{"-h", nodeIP, "info"}
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Info "+testCaseId)
}

func execPingTest(testCaseId string, nodeIP string, opt ...string) {
	result, _ := r.Ping(nodeIP, opt...)
	expectedArgList := []string{"-h", nodeIP, "ping"}
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Ping "+testCaseId)
}

func execClusterNodesTest(testCaseId string, nodeIP string, opt ...string) {
	_, result, _ := r.ClusterNodes(nodeIP, opt...)
	expectedArgList := []string{"-h", nodeIP, "cluster", "nodes"}
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster Nodes "+testCaseId)
}

func execMyClusterIDTest(testCaseId string, nodeIP string, opt ...string) {
	result, _ := r.MyClusterID(nodeIP, opt...)
	expectedArgLine := []string{"-h", nodeIP, "cluster", "myid"}
	expectedArgLine = r.Handler.buildCommand(r.Port, expectedArgLine, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgLine)
	resultHandler(expectedResult, result, "My Cluster ID "+testCaseId)
}

func execClusterForgetTest(testCaseId string, nodeIP string, forgetNodeID string, opt ...string) {
	result, _ := r.ClusterForget(nodeIP, forgetNodeID, opt...)
	expectedArgList := []string{"-h", nodeIP, "cluster", "forget", forgetNodeID}
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, opt...)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)
	resultHandler(expectedResult, result, "Cluster Forget "+testCaseId)
}
