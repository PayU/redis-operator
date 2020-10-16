package framework

import (
	"github.com/PayU/Redis-Operator/controllers/rediscli"
	"k8s.io/client-go/kubernetes"
)

type Framework struct {
	KubeClient kubernetes.Clientset
	RedisCLI   rediscli.RedisCLI
}

func New() (*Framework, error) {
	return &Framework{}, nil
}

// CreateRedisOperator creates a Redis Operator Kubernetes Deployment
func (f *Framework) CreateRedisOperator() error {
	return nil
}
