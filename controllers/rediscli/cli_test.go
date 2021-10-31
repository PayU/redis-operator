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

func resultHandler(expected string, result string, t *testing.T) {
	if strings.Compare(expected, result) != 0 {
		t.Fatalf("\nExpected result : %v\nActual result   : %v\nTest failed", expected, result)
	} else {
		t.Logf("Test 1 passed - Command: %v", result)
		fmt.Printf("Test 1 passed - Command: %v", result)
	}
}

func TestRedisCLI(t *testing.T) {
	r := &RedisCLI{nil, nil, "6380", nil}
	r.Port = "6381"
	r.Handler = &TestCommandHandler{}
	testClusterCreate(r, t)
}

func testClusterCreate(r *RedisCLI, t *testing.T) {
	// Test 1 : No routing port, No address ports, no optional arguments
	addresses := []string{"127.0.0.1", "128.1.1.2:", "129.2.2.3", "130.3.3.4:"}
	result, _ := r.ClusterCreate(addresses)

	updatedAddresses := addressesPortDecider(addresses, r.Port)
	expectedArgList := append([]string{"--cluster", "create"}, updatedAddresses...)
	expectedArgList = append(expectedArgList, "--cluster-yes")
	expectedArgList = r.Handler.buildCommand(r.Port, expectedArgList, r.Auth)
	expectedResult, _, _ := r.Handler.executeCommand(expectedArgList)

	resultHandler(expectedResult, result, t)

	//expectedAddresses := ""
	//expected := []string{"-p", r.Port, "--user", r.Auth.User, "--cluster", "create", addresses}

	// Test 2 : Routing port is provided, Only part of the address ports are provided, no other optional arguments
	// Test 3 : Routing port is provided, Only part of the addres ports are provided, other optional arguments are provided
	// Test 4 : Routing port is provided, All of the address ports are provided, other opional arguments are provided
}
