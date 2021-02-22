// +build e2e_redis_op

package framework

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"
	"strings"
	"time"

	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

func (f *Framework) runKustomizeCommand(command []string, path string) (string, string, error) {
	var sout, serr bytes.Buffer

	cmd := exec.Command("kustomize", command...)
	cmd.Dir = path
	cmd.Stdout = &sout
	cmd.Stderr = &serr
	err := cmd.Run()

	if err != nil {
		return sout.String(), serr.String(), errors.Wrapf(err, "kustomize incountered an error on command %s", command)
	} else if strings.TrimSpace(serr.String()) != "" {
		return sout.String(), serr.String(), errors.Errorf("kustomize encountered an error on command %s\n%s", command, serr.String())
	} else if strings.TrimSpace(sout.String()) == "" {
		return sout.String(), serr.String(), errors.Errorf("kustomize returned empty config for command %s", command)
	}

	return sout.String(), serr.String(), err
}

func (f *Framework) BuildKustomizeConfig(configPath string, image string, namespace string) ([]runtime.Object, map[string]string, error) {
	// TODO better, more generic approach to parsing the config is with unstructured objects:
	// https://github.com/kubernetes-sigs/controller-runtime/blob/v0.6.3/pkg/envtest/crd.go#L354
	fmt.Println("[E2E] Building kustomize config...")
	yamlMap := make(map[string]string)
	var configObjects []runtime.Object
	customResourceDefinition := extv1.CustomResourceDefinition{}
	crdGenerated := false

	editCmd := []string{"edit", "image", "controller=" + image, "namespace=" + namespace}
	sout, _, err := f.runKustomizeCommand(editCmd, "")
	if err != nil {
		return nil, nil, err
	}

	buildCmd := []string{"build", configPath}
	sout, _, err = f.runKustomizeCommand(buildCmd, "")
	if err != nil {
		return nil, nil, err
	}

	yamlRes, err := getDocumentsFromString(sout)
	if err != nil {
		return nil, nil, err
	}

	for _, res := range yamlRes {
		strRes := string(res)
		lines := strings.Split(strRes, "\n")
		kind := strings.Split(lines[1], ": ")[1]
		yamlMap[kind] = strRes
		switch kind {
		case "Namespace":
			namespace := corev1.Namespace{}
			err = yaml.Unmarshal(res, &namespace)
			configObjects = append(configObjects, &namespace)
		case "CustomResourceDefinition":
			crdGenerated = true
			err = yaml.Unmarshal(res, &customResourceDefinition)
		case "ServiceAccount":
			serviceAccount := corev1.ServiceAccount{}
			err = yaml.Unmarshal(res, &serviceAccount)
			configObjects = append(configObjects, &serviceAccount)
		case "Role":
			role := rbacv1.Role{}
			err = yaml.Unmarshal(res, &role)
			configObjects = append(configObjects, &role)
		case "ClusterRole":
			clusterRole := rbacv1.ClusterRole{}
			err = yaml.Unmarshal(res, &clusterRole)
			configObjects = append(configObjects, &clusterRole)
		case "RoleBinding":
			roleBinding := rbacv1.RoleBinding{}
			err = yaml.Unmarshal(res, &roleBinding)
			configObjects = append(configObjects, &roleBinding)
		case "ClusterRoleBinding":
			clusterRoleBinding := rbacv1.ClusterRoleBinding{}
			err = yaml.Unmarshal(res, &clusterRoleBinding)
			configObjects = append(configObjects, &clusterRoleBinding)
		case "Deployment":
			deployment := appsv1.Deployment{}
			err = yaml.Unmarshal(res, &deployment)
			configObjects = append(configObjects, &deployment)
		default:
			fmt.Printf("Kustomize genrated unsupported resource kind: %v\n", kind)
		}
		if err != nil {
			fmt.Printf("Could not unmarshal %v\n", kind)
			return nil, nil, err
		}
	}
	if crdGenerated {
		configObjects = append([]runtime.Object{&customResourceDefinition}, configObjects...)
	}

	return configObjects, yamlMap, nil
}

func (f *Framework) isKubectlWarning(serr string) bool {
	errMsg := strings.Split(strings.TrimSpace(serr), " ")
	if len(errMsg) > 0 && errMsg[0] == "Warning:" {
		return true
	}
	return false
}

func (f *Framework) kubectlPortForward(local string, remote string, pods ...corev1.Pod) error {
	var sout, serr bytes.Buffer
	for _, pod := range pods {
		fmt.Printf("Executing kubectl port forward: %s %s %s\n", pod.Namespace, "pod/"+pod.Name, local+":"+remote)
		cmd := exec.Command("kubectl", "port-forward", "-n", pod.Namespace, "pod/"+pod.Name, local+":"+remote)
		cmd.Stdout = &sout
		cmd.Stderr = &serr
		err := cmd.Start()
		if err != nil {
			fmt.Printf("[kubectl] kubectl port-forward failed: %v\n", err)
			return err
		}
		time.Sleep(time.Second * 2)
	}
	return nil
}

// Runs a command directly on a container via kubectl
// command: command to be run on the container given as list of string
func (f *Framework) kubectlContainerShell(pod corev1.Pod, container string, command ...string) (string, string, error) {
	// TODO should merge with executeKubectlCommand
	// redis-cli example: kubectl exec -n default -i -t redis-node-0 --container redis-container -- redis-cli info memory
	var sout, serr bytes.Buffer

	args := append([]string{"exec", "-n", pod.Namespace, "-i", pod.Name, "--container", container, "--"}, command...)
	cmd := exec.Command("kubectl", args...)

	cmd.Stdout = &sout
	cmd.Stderr = &serr

	err := cmd.Run()

	if err != nil {
		return sout.String(), serr.String(), errors.Wrap(err, fmt.Sprintf("kubectl command returned an error for command %v\ninput:\n", args))
		// return sout.String(), serr.String(), errors.Wrap(err, fmt.Sprintf("kubectl command returned an error for command %v\ninput:\n%v", args, yamlRes))
	} else if strings.TrimSpace(serr.String()) != "" && !f.isKubectlWarning(serr.String()) {
		fmt.Printf("Kubectl output: %s\n", serr.String())
		return sout.String(), serr.String(), errors.Errorf("kubectl command returned an error: %s", serr.String())
	}
	return sout.String(), serr.String(), err

}

func (f *Framework) executeKubectlCommand(yamlRes string, args []string, timeout time.Duration, dryRun bool) (string, string, error) {
	var sout, serr bytes.Buffer

	if dryRun {
		args = append([]string{"--dry-run=client", "-o", "yaml"}, args...)
	}
	args = append([]string{"--request-timeout", timeout.String()}, args...)
	cmd := exec.Command("kubectl", args...)
	cmd.Stdin = strings.NewReader(yamlRes)
	cmd.Stdout = &sout
	cmd.Stderr = &serr

	err := cmd.Run()

	if err != nil {
		return sout.String(), serr.String(), errors.Wrap(err, fmt.Sprintf("kubectl command returned an error for command %v\ninput:\n", args))
		// return sout.String(), serr.String(), errors.Wrap(err, fmt.Sprintf("kubectl command returned an error for command %v\ninput:\n%v", args, yamlRes))
	} else if strings.TrimSpace(serr.String()) != "" && !f.isKubectlWarning(serr.String()) {
		fmt.Printf("Kubectl output: %s\n", serr.String())
		return sout.String(), serr.String(), errors.Errorf("kubectl command returned an error: %s", serr.String())
	}
	return sout.String(), serr.String(), err
}

func (f *Framework) kubectlApply(yamlResource string, timeout time.Duration, dryRun bool) (string, string, error) {
	applyCmd := []string{"apply", "-f", "-"}
	return f.executeKubectlCommand(yamlResource, applyCmd, timeout, dryRun)
}

func (f *Framework) kubectlDelete(yamlResource string, timeout time.Duration) (string, string, error) {
	deleteCmd := []string{"delete", "-f", "-"}
	lines := strings.Split(yamlResource, "\n")
	kind := strings.Split(lines[1], ": ")[1]
	fmt.Printf("[E2E] kubectl delete %v\n", kind)
	return f.executeKubectlCommand(yamlResource, deleteCmd, timeout, false)
}

func (f *Framework) ApplyYAMLfile(filePath string, timeout time.Duration, dryRun bool) error {
	yamlRes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil
	}
	sout, serr, err := f.kubectlApply(string(yamlRes), timeout, dryRun)
	if err != nil || serr != "" {
		return errors.Wrapf(err, "kubectl apply failed out: %v | err: %v", sout, serr)
	}
	return nil
}

// get individual YAML documents from string
func getDocumentsFromString(res string) ([][]byte, error) {
	docs := [][]byte{}
	reader := k8syaml.NewYAMLReader(bufio.NewReader(strings.NewReader(res)))
	for {
		doc, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		docs = append(docs, doc)
	}

	return docs, nil
}
