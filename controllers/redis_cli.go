package controllers

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
)

/*
* executeCommand returns the exec command stdout response
* or an error strcut in case something goes wrong
 */
func executeCommand(log logr.Logger, args []string) (string, error) {
	var out bytes.Buffer
	var stderr bytes.Buffer

	cmd := exec.Command("/redis-cli", args...)
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	log.Info(fmt.Sprintf("executing redis-cli command:%v", args))
	err := cmd.Run()
	if err != nil {
		log.Error(err, fmt.Sprintf("unexpected error occurred when execute redis-cli:%s", stderr.String()))
		return "", err
	}

	log.Info(fmt.Sprintf("redis-cli command output:%s", out.String()))
	return out.String(), nil
}

func (r *RedisOperatorReconciler) redisCliClusterCreate(leaderPodIPAddresses []string) error {
	r.Log.Info(fmt.Sprintf("initializing redis cluster. leader addresses:%v", leaderPodIPAddresses))
	args := make([]string, 0)
	args = append(args, "--cluster", "create")

	for _, addr := range leaderPodIPAddresses {
		args = append(args, addr)
	}

	// this will run the command non-interactively
	args = append(args, "--cluster-yes")

	if _, err := executeCommand(r.Log, args); err != nil {
		nodeAddr := strings.Split(leaderPodIPAddresses[0], ":")
		// since operators are "live in the past", there is a chance that
		// the error is because the cluster already exists, so we are verifing it.
		// nodeAddr[0] == ip-address
		stateStdout, err := r.redisCliClusterInfo(nodeAddr[0])
		clusterExists := true

		if stateStdout != "" {
			lines := strings.Split(stateStdout, "\n")

			for _, lineInfo := range lines {
				// redis cluster line info is in form of key:value string
				keyValue := strings.Split(lineInfo, ":")

				if keyValue[0] == "cluster_state" {
					clusterExists = clusterExists && keyValue[1] == "ok"
				} else if keyValue[0] == "cluster_size" {
					clusterExists = clusterExists && keyValue[1] == strconv.Itoa(len(leaderPodIPAddresses))
				}
			}

			if clusterExists {
				err = nil
			}
		}

		if err != nil {
			r.Log.Info("init redis cluster was NOT finished successfully")
			return err
		}

	}

	r.Log.Info("redis cluster has initialized successfully")
	return nil
}

func (r *RedisOperatorReconciler) redisCliClusterInfo(nodeIP string) (string, error) {
	r.Log.Info(fmt.Sprintf("checking cluster info using redis-cli. node ip address:%s", nodeIP))
	args := make([]string, 0)
	args = append(args, "-h", nodeIP, "cluster", "info")

	stdout, err := executeCommand(r.Log, args)
	if err != nil {
		r.Log.Info("unable to check cluster state using redis-cli")
		return "", err
	}

	return stdout, nil
}
