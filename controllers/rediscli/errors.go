/* Error strings reference:
https://github.com/redis/redis/blob/unstable/src/redis-cli.c
https://github.com/redis/redis/blob/unstable/src/cluster.c
*/

package rediscli

import "strings"

func IsIDNotFound(err error) bool {
	keywords := "No such node ID"
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, strings.ToLower(keywords))
}

func IsNodeIsNotMaster(err error) bool {
	keywords := "The specified node is not a master"
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, strings.ToLower(keywords))
}
