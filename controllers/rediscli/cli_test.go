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
	if strings.Compare(expected, result) != 0 {
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

	//t.Fatal("For test")
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
