/* Error strings reference:
https://github.com/redis/redis/blob/unstable/src/redis-cli.c
https://github.com/redis/redis/blob/unstable/src/cluster.c
*/

package rediscli

import (
	"strings"

	"github.com/pkg/errors"
)

// https://github.com/redis/redis/blob/899c85ae67631f4f9f866a254ac5e149b86e5529/src/server.c#L2493
var SHARED_ERR_STRINGS = map[string]string{
	"wrongtype":     "WRONGTYPE Operation against a key holding the wrong kind of value",
	"nokeyerr":      "ERR no such key",
	"syntaxerr":     "ERR syntax error",
	"srcdsterr":     "ERR source and destination objects are the same",
	"indexrangeerr": "ERR index out of range",
	"noscript":      "NOSCRIPT No matching script. Please use EVAL",
	"loading":       "LOADING Redis is loading the dataset in memory",
	"busy":          "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE",
	"masterdown":    "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'",
	"misconf":       "MISCONF Redis is configured to save RDB snapshots, but it is currently not able to persist on disk",
	"readonly":      "READONLY You can't write against a read only replica",
	"noauth":        "NOAUTH Authentication required",
	"oom":           "OOM command not allowed when used memory > 'maxmemory'",
	"execabort":     "EXECABORT Transaction discarded because of previous errors",
	"noreplicas":    "NOREPLICAS Not enough good replicas to write",
	"busykey":       "BUSYKEY Target key name already exists",
}

var ERR_STRINGS = map[string]string{
	"idnotdef":        "No such node ID",
	"nodenotmaster":   "The specified node is not a master", // https://github.com/redis/redis/blob/29ac9aea5de2f395960834b262b3d94f9efedbd8/src/cluster.c#L4858
	"unknown":         "Unknown node",                       // https://github.com/redis/redis/blob/29ac9aea5de2f395960834b262b3d94f9efedbd8/src/cluster.c#L4601
	"failoverreplica": "ERR You should send CLUSTER FAILOVER to a replica",
}

func errorStringPrefix(err error, keywords string) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(strings.ToLower(err.Error()), strings.ToLower(keywords))
}

func errorStringMatch(err error, keywords string) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), strings.ToLower(keywords))
}

func IsNodeIsNotMaster(err error) bool {
	return errorStringMatch(err, ERR_STRINGS["nodenotmaster"])
}

func IsLoading(err error) bool {
	return errorStringMatch(err, SHARED_ERR_STRINGS["loading"])
}

func IsFailoverNotOnReplica(err error) bool {
	return errorStringMatch(err, ERR_STRINGS["failoverreplica"])
}

// Checks if an error is prefixed by the generic ERR string
func IsGenericError(err error) bool {
	return errorStringPrefix(err, "ERR") || errorStringPrefix(err, "\\[ERR\\]")
}

// Checks if a given string matches any supported redis-cli error types
func IsError(message string) bool {
	for _, errstr := range SHARED_ERR_STRINGS {
		if strings.Contains(strings.ToLower(message), strings.ToLower(errstr)) {
			return true
		}
	}
	for _, errstr := range ERR_STRINGS {
		if strings.Contains(strings.ToLower(message), strings.ToLower(errstr)) {
			return true
		}
	}
	if IsGenericError(errors.Errorf(message)) {
		return true
	}
	return false
}
