package data

import (
	"io/ioutil"
	"os"
)

func SaveRedisClusterView(data []byte) {
	fileName := os.Getenv("CLUSTER_VIEW_FILE")

	_ = ioutil.WriteFile(fileName, data, 0644)
}

func GetClusterView() ([]byte, error) {
	fileName := os.Getenv("CLUSTER_VIEW_FILE")

	return ioutil.ReadFile(fileName)
}

func SaveRedisClusterState(s string) {
	fileName := os.Getenv("CLUSTER_STATE_FILE")
	_ = ioutil.WriteFile(fileName, []byte(s), 0644)
}

func GetRedisClusterState() string {
	fileName := os.Getenv("CLUSTER_STATE_FILE")
	byteValue, err := ioutil.ReadFile(fileName)
	if err != nil {
		return "NotExists"
	}
	return string(byteValue)
}
