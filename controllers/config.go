package controllers

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/go-logr/logr"
	"gopkg.in/yaml.v2"
	"k8s.io/utils/inotify"
)

/*
# The thresholds value sets definite bounderies for the operator to perform during running concurrent operations
# and during decision making based on given stated values

# During new node initialization, a request for data replication is sent, and each new node is being sampled and watched untill threshold is reached
# in order to make sure the sync process is being performed properly
# SyncMatchThreshold

# During recovery process, missing pods will be recreated asynchronously,
# this value set the maximum unhealthy nodes that will be recovered by operator at once per reconcile loop
# MaxToleratedPodsRecoverAtOnce

# During updating process, pods get failed-over, removed, and recreated so the new ones will hold
# the new requested updated form in terms of the new spec.
# this value set the maximum number of nodes to be deleted at once per update loop
# MaxToleratedPodsUpdateAtOnce

*/

/*
# The wait times are defined by an interval value - how often the check is done
# and a timeout value, total amount of time to wait before considering the
# operation failed.

# Wait values for the SYNC operation.
# SyncCheckInterval
# SyncCheckTimeout

# Wait values for redis cluster configuration alignment.
# SleepDuringTablesAlignProcess
# RedisNodesAgreeAboutSlotsConfigCheckInterval
# RedisNodesAgreeAboutSlotsConfigTimeout

# Wait duration of the '--cluster create' command.
# ClusterCreateInterval
# ClusterCreateTimeout

# The estimated time it takes for volume mounted configmaps to be updated on the
# pods. After a configmap is changed, the configmap controller will update a
# hash annotation on the pod so kubelet will refresh the mounted volume.
# ACLFilePropagationDuration

# The estimated time it takes for Redis to load the new config map from disk.
# ACLFileLoadDuration

# Wait duration for a pod to be in ready state - pod is in Ready state and
# the containers passed all conditions.
# PodReadyCheckInterval
# PodReadyCheckTimeout

# Wait duration for the network of a pod to be up.
# PodNetworkCheckInterval
# PodNetworkCheckTimeout

# Wait duration for the deletion of a pod.
# PodDeleteCheckInterval
# PodDeleteCheckTimeout

# Wait duration for the removal of node id from other nodes tables
# RedisRemoveNodeCheckInterval
# RedisRemoveNodeTimeout

# Duration of the PING command.
# RedisPingCheckInterval
# RedisPingCheckTimeout

# Wait duration of the 'cluster replicas' command.
# RedisClusterReplicationCheckInterval
# RedisClusterReplicationCheckTimeout

# Wait duration for nodes to load dataset to their memory
# WaitForRedisLoadDataSetInMemoryCheckInterval
# WaitForRedisLoadDataSetInMemoryTimeout

# Wait duration of the MEET command.
# RedisClusterMeetCheckInterval
# RedisClusterMeetCheckTimeout

# Wait duration of the manual failover operation initialized by a
# 'cluster failover takeover' command.
# RedisManualFailoverCheckInterval
# RedisManualFailoverCheckTimeout

# Wait duration of the auto failover operation, initialized by Redis after a node
# failure.
# RedisAutoFailoverCheckInterval
# RedisAutoFailoverCheckTimeout

# SleepIfForgetNodeFails
# If forget node function fails, sleep before taking any deletion or irreversible action
*/

type RedisOperatorConfig struct {
	Path   string
	Log    logr.Logger
	Config OperatorConfig
}

type OperatorConfigThresholds struct {
	SyncMatchThreshold            int `yaml:"SyncMatchThreshold"`
	MaxToleratedPodsRecoverAtOnce int `yaml:"MaxToleratedPodsRecoverAtOnce"`
	MaxToleratedPodsUpdateAtOnce  int `yaml:"MaxToleratedPodsUpdateAtOnce"`
}

type OperatorConfigTimes struct {
	SyncCheckInterval                            time.Duration `yaml:"SyncCheckInterval"`
	SyncCheckTimeout                             time.Duration `yaml:"SyncCheckTimeout"`
	SleepDuringTablesAlignProcess                time.Duration `yaml:"SleepDuringTablesAlignProcess"`
	ClusterCreateInterval                        time.Duration `yaml:"ClusterCreateInterval"`
	ClusterCreateTimeout                         time.Duration `yaml:"ClusterCreateTimeout"`
	ACLFilePropagationDuration                   time.Duration `yaml:"ACLFilePropagationDuration"`
	ACLFileLoadDuration                          time.Duration `yaml:"ACLFileLoadDuration"`
	PodReadyCheckInterval                        time.Duration `yaml:"PodReadyCheckInterval"`
	PodReadyCheckTimeout                         time.Duration `yaml:"PodReadyCheckTimeout"`
	PodNetworkCheckInterval                      time.Duration `yaml:"PodNetworkCheckInterval"`
	PodNetworkCheckTimeout                       time.Duration `yaml:"PodNetworkCheckTimeout"`
	PodDeleteCheckInterval                       time.Duration `yaml:"PodDeleteCheckInterval"`
	PodDeleteCheckTimeout                        time.Duration `yaml:"PodDeleteCheckTimeout"`
	RedisPingCheckInterval                       time.Duration `yaml:"RedisPingCheckInterval"`
	RedisPingCheckTimeout                        time.Duration `yaml:"RedisPingCheckTimeout"`
	RedisClusterReplicationCheckInterval         time.Duration `yaml:"RedisClusterReplicationCheckInterval"`
	RedisClusterReplicationCheckTimeout          time.Duration `yaml:"RedisClusterReplicationCheckTimeout"`
	RedisClusterMeetCheckInterval                time.Duration `yaml:"RedisClusterMeetCheckInterval"`
	RedisClusterMeetCheckTimeout                 time.Duration `yaml:"RedisClusterMeetCheckTimeout"`
	RedisManualFailoverCheckInterval             time.Duration `yaml:"RedisManualFailoverCheckInterval"`
	RedisManualFailoverCheckTimeout              time.Duration `yaml:"RedisManualFailoverCheckTimeout"`
	RedisAutoFailoverCheckInterval               time.Duration `yaml:"RedisAutoFailoverCheckInterval"`
	RedisAutoFailoverCheckTimeout                time.Duration `yaml:"RedisAutoFailoverCheckTimeout"`
	RedisNodesAgreeAboutSlotsConfigTimeout       time.Duration `yaml:"RedisNodesAgreeAboutSlotsConfigTimeout"`
	RedisNodesAgreeAboutSlotsConfigCheckInterval time.Duration `yaml:"RedisNodesAgreeAboutSlotsConfigCheckInterval"`
	RedisRemoveNodeCheckInterval                 time.Duration `yaml:"RedisRemoveNodeCheckInterval"`
	RedisRemoveNodeTimeout                       time.Duration `yaml:"RedisRemoveNodeTimeout"`
	WaitForRedisLoadDataSetInMemoryCheckInterval time.Duration `yaml:"WaitForRedisLoadDataSetInMemoryCheckInterval"`
	WaitForRedisLoadDataSetInMemoryTimeout       time.Duration `yaml:"WaitForRedisLoadDataSetInMemoryTimeout"`
	SleepIfForgetNodeFails                       time.Duration `yaml:"SleepIfForgetNodeFails"`
}

type OperatorConfig struct {
	Times      OperatorConfigTimes      `yaml:"times"`
	Thresholds OperatorConfigThresholds `yaml:"thresholds"`
}

func NewRedisOperatorConfig(configPath string, logger logr.Logger) (*RedisOperatorConfig, error) {
	oc := new(RedisOperatorConfig)
	oc.Log = logger
	oc.Path = configPath
	oc.Config = OperatorConfig{}
	if err := oc.loadConfig(); err != nil {
		return nil, err
	}
	go oc.monitorConfigFile()
	return oc, nil
}

// Default constants used by the controllers for various tasks.
// It is used as fallback in case a "config/configfiles/operator.conf" configmap is not provided.
func DefaultRedisOperatorConfig(logger logr.Logger) *RedisOperatorConfig {
	return &RedisOperatorConfig{
		Log: logger,
		Config: OperatorConfig{
			Thresholds: OperatorConfigThresholds{
				SyncMatchThreshold:            90,
				MaxToleratedPodsRecoverAtOnce: 15,
				MaxToleratedPodsUpdateAtOnce:  5,
			},
			Times: OperatorConfigTimes{
				SyncCheckInterval:                            5 * 1000 * time.Millisecond,
				SyncCheckTimeout:                             30 * 1000 * time.Millisecond,
				SleepDuringTablesAlignProcess:                12 * 1000 * time.Millisecond,
				ClusterCreateInterval:                        5 * 1000 * time.Millisecond,
				ClusterCreateTimeout:                         90 * 1000 * time.Millisecond,
				ACLFilePropagationDuration:                   5 * 1000 * time.Millisecond,
				ACLFileLoadDuration:                          5 * 1000 * time.Millisecond,
				PodReadyCheckInterval:                        3 * 1000 * time.Millisecond,
				PodReadyCheckTimeout:                         30 * 1000 * time.Millisecond,
				PodNetworkCheckInterval:                      3 * 1000 * time.Millisecond,
				PodNetworkCheckTimeout:                       60 * 1000 * time.Millisecond,
				PodDeleteCheckInterval:                       3 * 1000 * time.Millisecond,
				PodDeleteCheckTimeout:                        60 * 1000 * time.Millisecond,
				RedisPingCheckInterval:                       2 * 1000 * time.Millisecond,
				RedisPingCheckTimeout:                        20 * 1000 * time.Millisecond,
				RedisClusterReplicationCheckInterval:         2 * 1000 * time.Millisecond,
				RedisClusterReplicationCheckTimeout:          30 * 1000 * time.Millisecond,
				RedisClusterMeetCheckInterval:                2 * 1000 * time.Millisecond,
				RedisClusterMeetCheckTimeout:                 10 * 1000 * time.Millisecond,
				RedisManualFailoverCheckInterval:             5 * 1000 * time.Millisecond,
				RedisManualFailoverCheckTimeout:              40 * 1000 * time.Millisecond,
				RedisAutoFailoverCheckInterval:               5 * 1000 * time.Millisecond,
				RedisAutoFailoverCheckTimeout:                40 * 1000 * time.Millisecond,
				RedisNodesAgreeAboutSlotsConfigCheckInterval: 3 * 1000 * time.Millisecond,
				RedisNodesAgreeAboutSlotsConfigTimeout:       12 * 1000 * time.Millisecond,
				RedisRemoveNodeCheckInterval:                 2 * 1000 * time.Millisecond,
				RedisRemoveNodeTimeout:                       20 * 1000 * time.Millisecond,
				WaitForRedisLoadDataSetInMemoryCheckInterval: 2 * 1000 * time.Millisecond,
				WaitForRedisLoadDataSetInMemoryTimeout:       10 * 1000 * time.Millisecond,
				SleepIfForgetNodeFails:                       20 * 1000 * time.Millisecond,
			},
		},
	}
}

func (r *RedisOperatorConfig) loadConfig() error {
	buff, err := ioutil.ReadFile(r.Path)
	if err != nil {
		r.Log.Error(err, "Failed to read the file")
		return err
	}
	if err := yaml.Unmarshal(buff, &(r.Config)); err != nil {
		return err
	}
	r.Log.Info(fmt.Sprintf("Loaded config: %+v", r.Config))
	return nil
}

func (r *RedisOperatorConfig) getFileWatcher() (*inotify.Watcher, error) {
	watcher, err := inotify.NewWatcher()
	if err != nil {
		r.Log.Error(err, "Failed to start new config file watcher")
		return nil, err
	}
	err = watcher.Watch(r.Path)
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("Failed to watch config file at %s", r.Path))
		return nil, err
	}
	return watcher, nil
}

func (r *RedisOperatorConfig) monitorConfigFile() error {
	watcher, err := r.getFileWatcher()
	if err != nil {
		return err
	}
	defer r.Log.Info(fmt.Sprintf("Operator config watcher exiting..."))
	for {
		select {
		case ev := <-watcher.Event:
			if ev.Mask&inotify.InDeleteSelf != 0 || ev.Mask&inotify.InModify != 0 {
				r.Log.Info(fmt.Sprintf("Operator config file changed on disk. Reloading..."))
				r.loadConfig()
			}
			if ev.Mask&inotify.InIgnored != 0 {
				r.Log.Info("Watching new config file")
				watcher, err = r.getFileWatcher()
				if err != nil {
					r.Log.Error(err, fmt.Sprintf("Failed to watch config file at %s", r.Path))
				}
			}
		case err := <-watcher.Error:
			r.Log.Error(err, "Config file watcher failed")
		}
	}
}
