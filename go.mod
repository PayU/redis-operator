module github.com/PayU/redis-operator

go 1.16

require (
	github.com/go-logr/logr v0.1.0
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-test/deep v1.0.7
	github.com/labstack/echo/v4 v4.6.1
	github.com/pkg/errors v0.9.1
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.18.6
	k8s.io/apiextensions-apiserver v0.18.6
	k8s.io/apimachinery v0.18.6
	k8s.io/client-go v0.18.6
	k8s.io/kubectl v0.18.6
	k8s.io/utils v0.0.0-20200603063816-c1c6865ac451
	sigs.k8s.io/controller-runtime v0.6.3
	sigs.k8s.io/yaml v1.2.0
)
