package rediscli

import (
	"strings"
)

// Important: the struct will not preserve the order of remove and add
// operations, please group all removes and adds together
type RedisACLCommands struct {
	Commands    []string
	RmCommands  []string
	AllCommands bool
	NoCommands  bool
}

type RedisACLKeys struct {
	Patterns  []string
	AllKeys   bool
	ResetKeys bool
}

type RedisACLChannels struct {
	Patterns      []string
	AllChannels   bool
	ResetChannels bool
}

type RedisACLPasswords struct {
	Passwords   []string
	RmPasswords []string
	Hashes      []string
	RmHashes    []string
	NoPass      bool
	ResetPass   bool
}

type RedisACLUser struct {
	Name      string
	On        bool
	Reset     bool
	Commands  RedisACLCommands
	Keys      RedisACLKeys
	Channels  RedisACLChannels
	Passwords RedisACLPasswords
}

type RedisACL struct {
	Users []RedisACLUser
}

func parseACL(rawData string) (RedisACL, error) {
	aclList := make([][]string, 0)
	acl := RedisACL{
		Users: []RedisACLUser{},
	}
	entries := strings.Split(rawData, "\n")
	for i, e := range entries {
		user := RedisACLUser{}
		entries[i] = strings.TrimLeft(e, "0123456789) \"")
		entries[i] = entries[i][:len(entries[i])-1]
		aclList = append(aclList, strings.Split(entries[i], " "))
		user.Name = aclList[i][1]
		for j, e := range aclList[i] {
			switch e[0] {
			case '>':
				user.Passwords.Passwords = append(user.Passwords.Passwords, aclList[i][j][1:])
			case '<':
				user.Passwords.RmPasswords = append(user.Passwords.RmPasswords, aclList[i][j][1:])
			case '#':
				user.Passwords.Hashes = append(user.Passwords.Hashes, aclList[i][j][1:])
			case '!':
				user.Passwords.RmHashes = append(user.Passwords.RmHashes, aclList[i][j][1:])
			case '+':
				user.Commands.Commands = append(user.Commands.Commands, aclList[i][j][1:])
			case '-':
				user.Commands.RmCommands = append(user.Commands.RmCommands, aclList[i][j][1:])
			case '&':
				user.Channels.Patterns = append(user.Channels.Patterns, aclList[i][j][1:])
			case '~':
				user.Keys.Patterns = append(user.Keys.Patterns, aclList[i][j][1:])
			default:
				switch e {
				case "on":
					user.On = true
				case "off":
					user.On = false
				case "allcommands":
					user.Commands.AllCommands = true
				case "nocommands":
					user.Commands.NoCommands = true
				case "allkeys":
					user.Keys.AllKeys = true
				case "resetkeys":
					user.Keys.ResetKeys = true
				case "allchannels":
					user.Channels.AllChannels = true
				case "resetchannels":
					user.Channels.ResetChannels = true
				case "nopass":
					user.Passwords.NoPass = true
				case "resetpass":
					user.Passwords.ResetPass = true
				case "reset":
					user.Reset = true
				default:
					// TODO should treat unmatched content
				}
			}
		}
		acl.Users = append(acl.Users, user)
	}
	return acl, nil
}

func NewRedisACL(rawData string) (RedisACL, error) {
	return parseACL(rawData)
}
