package rediscli

import (
	"fmt"
	"strings"
	"testing"
)

var redisCli *RedisCLI
var test *testing.T

func TestCli(t *testing.T) {
	test = t
	redisCli = NewRedisCLI(nil)
	redisCli.DefaultPort = "6380"
	redisCli.Auth.User = "testuser"
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
	testClusterFlushAll()
	testClusterReplicate()
	testACLLoad()
	testACLList()
}

func testClusterCreate() {

	fmt.Println("Running ClusterCreate unit test")

	// Test 1 : Routing port provided, only part of the provided leader addresses list contained ports
	addresses := []string{"127.0.0.1", "127.0.1.1:6379", "128.1.1.2:", "127.0.0.1:8080"}
	h := redisCli.ClusterCreate(addresses, "-p", "6390")
	result := h.Command
	expected := "-p 6390 --user testuser --cluster create 127.0.0.1:" + redisCli.DefaultPort + " 127.0.1.1:6379 128.1.1.2:" + redisCli.DefaultPort + " 127.0.0.1:8080 --cluster-yes"

	if strings.Compare(expected, result) != 0 {
		test.Fatalf("Test 1 :\nExpected: %s\nResult: %s\n", expected, result)
	}
	fmt.Println("Test 1 : Routing port provided, only part of the provided leader addresses list contained ports - Passed")

	// Test 2 : Routing port is not provided, only part of the provided leader addresses list contained ports
	h = redisCli.ClusterCreate(addresses)
	result = h.Command
	expected = "-p " + redisCli.DefaultPort + " --user testuser --cluster create 127.0.0.1:" + redisCli.DefaultPort + " 127.0.1.1:6379 128.1.1.2:" + redisCli.DefaultPort + " 127.0.0.1:8080 --cluster-yes"

	if strings.Compare(expected, result) != 0 {
		test.Fatalf("Test 2 :\nExpected: %s\nResult: %s\n", expected, result)
	}
	fmt.Println("Test 2 : Routing port is not provided, only part of the provided leader addresses list contained ports - Passed")

	// Test 3 : Routing port is not provided, only part of the provided leader addresses list contained ports, additional arguments provided
	h = redisCli.ClusterCreate(addresses, "-optionalFlag", "optionalVal", "-optionalFlag2", "optionalVal2")
	result = h.Command
	expected = "-p " + redisCli.DefaultPort + " --user testuser --cluster create 127.0.0.1:" + redisCli.DefaultPort + " 127.0.1.1:6379 128.1.1.2:" + redisCli.DefaultPort + " 127.0.0.1:8080 -optionalFlag optionalVal -optionalFlag2 optionalVal2 --cluster-yes"

	if strings.Compare(expected, result) != 0 {
		test.Fatalf("Test 3 :\nExpected: %s\nResult: %s\n", expected, result)
	}
	fmt.Println("Test 3 : Routing port is not provided, only part of the provided leader addresses list contained ports, additional arguments provided - Passed")

	// Test 4 : Routing port is provided, only part of the provided leader addresses list contained ports, additional arguments provided
	h = redisCli.ClusterCreate(addresses, "-p", "9090", "-optionalFlag", "optionalVal", "-optionalFlag2", "optionalVal2")
	result = h.Command
	expected = "-p 9090 --user testuser --cluster create 127.0.0.1:" + redisCli.DefaultPort + " 127.0.1.1:6379 128.1.1.2:" + redisCli.DefaultPort + " 127.0.0.1:8080 -optionalFlag optionalVal -optionalFlag2 optionalVal2 --cluster-yes"

	if strings.Compare(expected, result) != 0 {
		test.Fatalf("Test 4 :\nExpected: %s\nResult: %s\n", expected, result)
	}
	fmt.Println("Test 3 : Routing port is provided, only part of the provided leader addresses list contained ports, additional arguments provided - Passed")
}

func testClusterCheck() {}

func testAddFollower() {}

func testDelNode() {}

func testClusterInfo() {}

func testInfo() {}

func testPing() {}

func testClusterNodes() {}

func testMyClusterID() {}

func testClusterForget() {}

func testClusterReplicas() {}

func testClusterFailOver() {}

func testClusterMeet() {}

func testClusterReset() {}

func testClusterFlushAll() {}

func testClusterReplicate() {}

func testACLLoad() {}

func testACLList() {}
