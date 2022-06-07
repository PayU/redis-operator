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
# The wait times are defined by an interval value - how often the check is done
# and a timeout value, total amount of time to wait before considering the
# operation failed.

# Wait duration for the SYNC operation start. After a new node connects to a leader
# there can be a delay before the sync operation starts.
# SyncStartCheckInterval
# SyncStartCheckTimeout

# Wait duration for the SYNC operation.
# SyncCheckInterval
# SyncCheckTimeout

# Wait duration for the LOAD operation start.
# LoadStartCheckInterval
# LoadStartCheckTimeout

# Wait duration for the LOAD operation. This time should be set reasonably high
# because it depends on the size of the DB shards and network latency. Make sure
# the time is high enough to allow for the data transfer between two nodes.
# The LOAD and SYNC operations are important during the recreation of a lost
# node, when the data from a leader is loaded on a replica.
# https://redis.io/topics/replication
# The operator uses the INFO message from Redis to get information about the
# status of SYNC (master_sync_in_progress) and LOAD (loading_eta_seconds)
# https://redis.io/commands/info
# LoadCheckInterval
# LoadCheckTimeout

# The estimated time it takes for volume mounted configmaps to be updated on the
# pods. After a configmap is changed, the configmap controller will update a
# hash annotation on the pod so kubelet will refresh the mounted volume.
# ACLFilePropagationDuration

# The estimated time it takes for Redis to load the new config map from disk.
# ACLFileLoadDuration

# Wait duration of the '--cluster create' command.
# ClusterCreateInterval
# ClusterCreateTimeout

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

# Duration of the PING command.
# RedisPingCheckInterval
# RedisPingCheckTimeout

# Wait duration of the 'cluster replicas' command.
# RedisClusterReplicationCheckInterval
# RedisClusterReplicationCheckTimeout

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
*/

type RedisOperatorConfig struct {
	Path   string
	Log    logr.Logger
	Config OperatorConfig
}

type OperatorConfigTimes struct {
	SyncStartCheckInterval                       time.Duration `yaml:"syncStartCheckInterval"`
	SyncStartCheckTimeout                        time.Duration `yaml:"syncStartCheckTimeout"`
	SyncCheckInterval                            time.Duration `yaml:"syncCheckInterval"`
	SyncCheckTimeout                             time.Duration `yaml:"syncCheckTimeout"`
	LoadStartCheckInterval                       time.Duration `yaml:"loadStartCheckInterval"`
	LoadStartCheckTimeout                        time.Duration `yaml:"loadStartCheckTimeout"`
	LoadCheckInterval                            time.Duration `yaml:"loadCheckInterval"`
	LoadCheckTimeout                             time.Duration `yaml:"loadCheckTimeout"`
	ClusterCreateInterval                        time.Duration `yaml:"clusterCreateInterval"`
	ClusterCreateTimeout                         time.Duration `yaml:"clusterCreateTimeout"`
	ACLFilePropagationDuration                   time.Duration `yaml:"aclFilePropagationDuration"`
	ACLFileLoadDuration                          time.Duration `yaml:"aclFileLoadDuration"`
	PodReadyCheckInterval                        time.Duration `yaml:"podReadyCheckInterval"`
	PodReadyCheckTimeout                         time.Duration `yaml:"podReadyCheckTimeout"`
	PodNetworkCheckInterval                      time.Duration `yaml:"podNetworkCheckInterval"`
	PodNetworkCheckTimeout                       time.Duration `yaml:"podNetworkCheckTimeout"`
	PodDeleteCheckInterval                       time.Duration `yaml:"podDeleteCheckInterval"`
	PodDeleteCheckTimeout                        time.Duration `yaml:"podDeleteCheckTimeout"`
	RedisPingCheckInterval                       time.Duration `yaml:"redisPingCheckInterval"`
	RedisPingCheckTimeout                        time.Duration `yaml:"redisPingCheckTimeout"`
	RedisClusterReplicationCheckInterval         time.Duration `yaml:"redisClusterReplicationCheckInterval"`
	RedisClusterReplicationCheckTimeout          time.Duration `yaml:"redisClusterReplicationCheckTimeout"`
	RedisClusterMeetCheckInterval                time.Duration `yaml:"redisClusterMeetCheckInterval"`
	RedisClusterMeetCheckTimeout                 time.Duration `yaml:"redisClusterMeetCheckTimeout"`
	RedisManualFailoverCheckInterval             time.Duration `yaml:"redisManualFailoverCheckInterval"`
	RedisManualFailoverCheckTimeout              time.Duration `yaml:"redisManualFailoverCheckTimeout"`
	RedisAutoFailoverCheckInterval               time.Duration `yaml:"redisAutoFailoverCheckInterval"`
	RedisAutoFailoverCheckTimeout                time.Duration `yaml:"redisAutoFailoverCheckTimeout"`
	RedisNodesAgreeAboutSlotsConfigTimeout       time.Duration `yaml:"RedisNodesAgreeAboutSlotsConfigTimeout"`
	RedisNodesAgreeAboutSlotsConfigCheckInterval time.Duration `yaml:"RedisNodesAgreeAboutSlotsConfigCheckInterval"`
	RedisRemoveNodeCheckInterval                 time.Duration `yaml:"RedisRemoveNodeCheckInterval"`
	RedisRemoveNodeTimeout                       time.Duration `yaml:"RedisRemoveNodeTimeout"`
}

type OperatorConfig struct {
	Times OperatorConfigTimes `yaml:"times"`
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
// It is used as fallback in case a configmap is not provided.
func DefaultRedisOperatorConfig(logger logr.Logger) *RedisOperatorConfig {
	return &RedisOperatorConfig{
		Log: logger,
		Config: OperatorConfig{
			Times: OperatorConfigTimes{
				SyncStartCheckInterval:                       500 * time.Millisecond,
				SyncStartCheckTimeout:                        15000 * time.Millisecond,
				SyncCheckInterval:                            500 * time.Millisecond,
				SyncCheckTimeout:                             15000 * time.Millisecond,
				LoadStartCheckInterval:                       500 * time.Millisecond,
				LoadStartCheckTimeout:                        180000 * time.Millisecond,
				LoadCheckInterval:                            500 * time.Millisecond,
				LoadCheckTimeout:                             180000 * time.Millisecond,
				ClusterCreateInterval:                        5000 * time.Millisecond,
				ClusterCreateTimeout:                         80000 * time.Millisecond,
				ACLFilePropagationDuration:                   5000 * time.Millisecond,
				ACLFileLoadDuration:                          500 * time.Millisecond,
				PodReadyCheckInterval:                        4000 * time.Millisecond,
				PodReadyCheckTimeout:                         2880000 * time.Millisecond,
				PodNetworkCheckInterval:                      4000 * time.Millisecond,
				PodNetworkCheckTimeout:                       2880000 * time.Millisecond,
				PodDeleteCheckInterval:                       2000 * time.Millisecond,
				PodDeleteCheckTimeout:                        40000 * time.Millisecond,
				RedisPingCheckInterval:                       2000 * time.Millisecond,
				RedisPingCheckTimeout:                        300000 * time.Millisecond,
				RedisClusterReplicationCheckInterval:         2000 * time.Millisecond,
				RedisClusterReplicationCheckTimeout:          30000 * time.Millisecond,
				RedisClusterMeetCheckInterval:                2000 * time.Millisecond,
				RedisClusterMeetCheckTimeout:                 90000 * time.Millisecond,
				RedisManualFailoverCheckInterval:             2000 * time.Millisecond,
				RedisManualFailoverCheckTimeout:              30000 * time.Millisecond,
				RedisAutoFailoverCheckInterval:               2000 * time.Millisecond,
				RedisAutoFailoverCheckTimeout:                30000 * time.Millisecond,
				RedisNodesAgreeAboutSlotsConfigTimeout:       200000 * time.Millisecond,
				RedisNodesAgreeAboutSlotsConfigCheckInterval: 50000 * time.Millisecond,
				RedisRemoveNodeCheckInterval:                 20000 * time.Millisecond,
				RedisRemoveNodeTimeout:                       60000 * time.Millisecond,
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
