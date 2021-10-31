package rediscli

import (
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

func resultHandler(expected string, result string, t *testing.T, testCase string) {
	if strings.Compare(expected, result) != 0 {
		t.Fatalf("\nExpected result : %v\nActual result   : %v\nTest case %v failed", expected, result, testCase)
	}
}

func TestRedisCLI(t *testing.T) {
	r := &RedisCLI{nil, nil, "6380", nil}
	r.Handler = &TestCommandHandler{}

	testClusterCreate(r, t)
	testClusterCheck(r, t)
}

func testClusterCreate(r *RedisCLI, t *testing.T) {
	// Test 1 : Routing port is not provided, Address ports are not provided, no optional arguments
	addresses := []string{"127.0.0.1", "128.1.1.2:", "129.2.2.3", "130.3.3.4:"}
	result, _ := r.ClusterCreate(addresses)

	updatedAddresses := addressesPortDecider(addresses, r.Port)
	expectedArgList := append([]string{"--cluster", "create"}, updatedAddresses...)
	expectedArgList = append(expectedArgList, "--cluster-yes")
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)

	resultHandler(expectedResult, result, t, "Cluster Create")

	// Test 2 : Routing port is provided, Only part of the address ports are provided, no other optional arguments
	providedRoutingPort := "6379"
	optinalArgsLine := "-p " + providedRoutingPort
	addresses = []string{"127.0.0.1:8080", "128.1.1.2:6379", "129.2.2.3", "130.3.3.4:"}
	result, _ = r.ClusterCreate(addresses, optinalArgsLine)

	updatedAddresses = addressesPortDecider(addresses, r.Port)
	expectedArgList = append([]string{"--cluster", "create"}, updatedAddresses...)
	expectedArgList = append(expectedArgList, "--cluster-yes")
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, optinalArgsLine)
	expectedResult, _, _ = r.Handler.executeCommand(expectedArgList)

	resultHandler(expectedResult, result, t, "Cluster Create")

	// Test 3 : Routing port is provided, Only part of the addres ports are provided, other optional arguments are provided
	providedRoutingPort = "6375"
	optinalArgsLine = "-p " + providedRoutingPort + "-optArg optArgVal"
	addresses = []string{"127.0.0.1:8080", "128.1.1.2:6379", "129.2.2.3", "130.3.3.4:"}
	result, _ = r.ClusterCreate(addresses, optinalArgsLine)

	updatedAddresses = addressesPortDecider(addresses, r.Port)
	expectedArgList = append([]string{"--cluster", "create"}, updatedAddresses...)
	expectedArgList = append(expectedArgList, "--cluster-yes")
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, optinalArgsLine)
	expectedResult, _, _ = r.Handler.executeCommand(expectedArgList)

	resultHandler(expectedResult, result, t, "Cluster Create")

	// Test 4 : Routing port is provided, All of the address ports are provided, other opional arguments are provided (routing port is not the first optional arg among them)
	providedRoutingPort = "6363"
	optinalArgsLine = "optArg1 optVal1 -p " + providedRoutingPort + "-optArg2 optArgVal2"
	addresses = []string{"127.0.0.1:8090", "128.1.1.2:8090", "129.2.2.3:8090", "130.3.3.4:8090"}
	result, _ = r.ClusterCreate(addresses, optinalArgsLine)

	updatedAddresses = addressesPortDecider(addresses, r.Port)
	expectedArgList = append([]string{"--cluster", "create"}, updatedAddresses...)
	expectedArgList = append(expectedArgList, "--cluster-yes")
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, optinalArgsLine)
	expectedResult, _, _ = r.Handler.executeCommand(expectedArgList)

	resultHandler(expectedResult, result, t, "Cluster Create")

	// Test 5 : Routing port is provided, Address ports are not provided, optional arguments are given as a parametrized list
	providedRoutingPort = "6363"
	addresses = []string{"127.0.0.1:", "128.1.1.2:", "129.2.2.3", "130.3.3.4:"}
	result, _ = r.ClusterCreate(addresses, "-p "+providedRoutingPort, "-optArg optVal")

	updatedAddresses = addressesPortDecider(addresses, r.Port)
	expectedArgList = append([]string{"--cluster", "create"}, updatedAddresses...)
	expectedArgList = append(expectedArgList, "--cluster-yes")
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, "-p "+providedRoutingPort, "-optArg optVal")
	expectedResult, _, _ = r.Handler.executeCommand(expectedArgList)

	resultHandler(expectedResult, result, t, "Cluster Create")
}

func testClusterCheck(r *RedisCLI, t *testing.T) {
	// Test 1 : Routing port is not provided, adress port is not provided, no optional arguments
	address := "127.0.0.1"
	result, _ := r.ClusterCheck(address)

	expectedArgList := []string{"--cluster", "check", addressPortDecider(address, r.Port)}
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)

	resultHandler(expectedResult, result, t, "Cluster Check")

	// Test 2 : Routing port is provided, address port is not provided, no optional arguments
	providedRoutingPort := "6379"
	optinalArgsLine := "-p " + providedRoutingPort
	address = "127.0.0.1:"
	result, _ = r.ClusterCheck(address, optinalArgsLine)

	expectedArgList = []string{"--cluster", "check", addressPortDecider(address, r.Port)}
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, optinalArgsLine)
	expectedResult, _, _ = r.Handler.executeCommand(expectedArgList)

	resultHandler(expectedResult, result, t, "Cluster Check")

	// Test 3 : Routing port is not provided, address port is provided, optional arguments provided
	optinalArgsLine = "-optArg optArgVal"
	address = "127.0.0.1:6397"
	result, _ = r.ClusterCheck(address, optinalArgsLine)

	expectedArgList = []string{"--cluster", "check", addressPortDecider(address, r.Port)}
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, optinalArgsLine)
	expectedResult, _, _ = r.Handler.executeCommand(expectedArgList)

	resultHandler(expectedResult, result, t, "Cluster Check")

	// Test 4 : Routing port is provided, adress port is provided, optional arguments provided
	providedRoutingPort = "6379"
	optinalArgsLine = "-p " + providedRoutingPort + " -optArg optArgVal"
	address = "127.0.0.1:6398"
	result, _ = r.ClusterCheck(address, optinalArgsLine)

	expectedArgList = []string{"--cluster", "check", addressPortDecider(address, r.Port)}
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, optinalArgsLine)
	expectedResult, _, _ = r.Handler.executeCommand(expectedArgList)

	resultHandler(expectedResult, result, t, "Cluster Check")

	// Test 5 : Routing port is provided, Address port is not provided, optional arguments are given as a parametrized list
	providedRoutingPort = "6363"
	address = "127.0.0.1:"
	result, _ = r.ClusterCheck(address, "-p "+providedRoutingPort, "-optArg optVal")

	expectedArgList = []string{"--cluster", "check", addressPortDecider(address, r.Port)}
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth, "-p "+providedRoutingPort, "-optArg optVal")
	expectedResult, _, _ = r.Handler.executeCommand(expectedArgList)

	resultHandler(expectedResult, result, t, "Cluster Check")
}
