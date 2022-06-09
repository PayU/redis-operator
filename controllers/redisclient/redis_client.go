package redisclient

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/PayU/redis-operator/controllers/rediscli"
	"github.com/PayU/redis-operator/controllers/view"
	"github.com/go-redis/redis/v8"
)

type RedisClusterClient struct {
	clients map[string]*redis.Client
}

var clusterClient *RedisClusterClient = nil

var format string
var comp *regexp.Regexp

var lookups int = 5

func GetRedisClusterClient(v *view.RedisClusterView, cli *rediscli.RedisCLI) *RedisClusterClient {
	format = "MOVED\\s*\\d+\\s*(\\d+\\.\\d+\\.\\d+\\.\\d+:\\d+)"
	comp = regexp.MustCompile(format)
	if clusterClient == nil {
		clusterClient = &RedisClusterClient{
			clients: map[string]*redis.Client{},
		}
	}
	mutex := &sync.Mutex{}
	for _, n := range v.Nodes {
		if n == nil {
			continue
		}
		mutex.Lock()
		nodes, _, err := cli.ClusterNodes(n.Ip)
		if err != nil || nodes == nil || len(*nodes) <= 1 {
			continue
		}
		addr := n.Ip + ":" + cli.Port
		clusterClient.clients[addr] = redis.NewClient(&redis.Options{
			Addr:     addr,
			Username: "admin",
			Password: "adminpass",
		})
		mutex.Unlock()
	}
	return clusterClient
}

func (c *RedisClusterClient) Set(key string, val interface{}, retries int) error {
	if retries == 0 {
		return errors.New(fmt.Sprintf("Could not set key [%v], val [%v] into cluster, all nodes errored during attempt", key, val))
	}
	ctx := context.Background()
	for addr := range c.clients {
		e := c.set(ctx, key, val, addr, lookups)
		if e == nil {
			return nil
		}
	}
	return c.Set(key, val, retries-1)
}

func (c *RedisClusterClient) set(ctx context.Context, key string, val interface{}, addr string, lookups int) error {
	if lookups == 0 {
		return errors.New(fmt.Sprintf("Could not write data row [%v, %v]", key, val))
	}
	mutex := &sync.Mutex{}
	mutex.Lock()
	client, exists := c.clients[addr]
	mutex.Unlock()
	if !exists || client == nil {
		return errors.New(fmt.Sprintf("Client [%v] doesnt exists", addr))
	}
	_, err := client.Set(ctx, key, val, 0).Result()
	if err != nil {
		if strings.Contains(err.Error(), "MOVED") {
			a := c.extractAddress(err.Error())
			return c.set(ctx, key, val, a, lookups-1)
		}
		if strings.Contains(err.Error(), "i/o timeout") {
			c.clients[addr] = nil
		}
	}
	return err
}

func (c *RedisClusterClient) Get(key string, retries int) (value string, err error) {
	if retries == 0 {
		return "", errors.New(fmt.Sprintf("Could not extract key [%v]", key))
	}

	ctx := context.Background()
	for addr := range c.clients {
		v, e := c.get(ctx, key, addr, lookups)
		if e == nil {
			return v, nil
		}
	}
	return c.Get(key, retries-1)
}

func (c *RedisClusterClient) get(ctx context.Context, key string, addr string, lookups int) (value string, err error) {
	if lookups == 0 {
		return "", errors.New(fmt.Sprintf("Could not extract key [%v]", key))
	}
	mutex := &sync.Mutex{}
	mutex.Lock()
	client, exists := c.clients[addr]
	mutex.Unlock()
	if !exists || client == nil {
		return "", errors.New(fmt.Sprintf("Client [%v] doesnt exists", addr))
	}
	value, err = client.Get(ctx, key).Result()
	if err != nil {
		if strings.Contains(err.Error(), "nil") {
			err = nil
		} else {
			if strings.Contains(err.Error(), "MOVED") {
				a := c.extractAddress(err.Error())
				return c.get(ctx, key, a, lookups-1)
			}
			if strings.Contains(err.Error(), "i/o timeout") {
				c.clients[addr] = nil
				return value, err
			}
			return value, err
		}
	}
	return value, err
}

func (c *RedisClusterClient) extractAddress(msg string) string {
	matchingStrings := comp.FindAllStringSubmatch(msg, -1)
	for _, match := range matchingStrings {
		if len(match) > 1 {
			if len(match[1]) > 0 {
				return match[1]
			}
		}
	}
	return ""
}

func (c *RedisClusterClient) FlushAllData() {
	ctx := context.Background()
	for _, client := range c.clients {
		if client != nil {
			client.FlushAll(ctx)
		}
	}
}
