package rediscli

import (
	"crypto/sha256"
	"fmt"
	"sort"
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
	Hashes    []string
	RmHashes  []string
	NoPass    bool
	ResetPass bool
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

type ByName []RedisACLUser

func (a ByName) Len() int           { return len(a) }
func (a ByName) Less(i, j int) bool { return a[i].Name < a[j].Name }
func (a ByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type RedisACL struct {
	Users []RedisACLUser
}

func SortACLFields(acl *RedisACL) {
	for i := range acl.Users {
		sort.Strings(acl.Users[i].Passwords.Hashes)
		sort.Strings(acl.Users[i].Passwords.RmHashes)
		sort.Strings(acl.Users[i].Channels.Patterns)
		sort.Strings(acl.Users[i].Keys.Patterns)
		sort.Strings(acl.Users[i].Commands.Commands)
		sort.Strings(acl.Users[i].Commands.RmCommands)
	}
	sort.Sort(ByName(acl.Users))
}

func parseACL(rawData string) (*RedisACL, error) {
	aclList := make([][]string, 0)
	acl := RedisACL{
		Users: []RedisACLUser{},
	}
	rawData = strings.Trim(rawData, "\n")
	entries := strings.Split(rawData, "\n")
	for i, e := range entries {
		fmt.Printf("ACL entrie: %s\n", e)
		user := RedisACLUser{}
		entries[i] = strings.TrimLeft(e, "0123456789) \"")
		entries[i] = strings.Trim(entries[i], "\" ")
		aclList = append(aclList, strings.Split(entries[i], " "))
		user.Name = aclList[i][1]
		for j, e := range aclList[i] {
			switch e[0] {
			case '>':
				user.Passwords.Hashes = append(user.Passwords.Hashes, fmt.Sprintf("%x", sha256.Sum256([]byte(e[1:]))))
			case '<':
				user.Passwords.RmHashes = append(user.Passwords.RmHashes, fmt.Sprintf("%x", sha256.Sum256([]byte(e[1:]))))
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

	SortACLFields(&acl)

	return &acl, nil
}

func NewRedisACL(rawData string) (*RedisACL, error) {
	return parseACL(rawData)
}

func (t *RedisACLChannels) String() string {
	result := ""
	for _, pattern := range t.Patterns {
		result += fmt.Sprintf("&%s ", pattern)
	}
	if t.AllChannels {
		result += "allchannels "
	}
	// the string version will ignore the resetchannels because it can be set by
	// default in redis.conf and all users will have it unless otherwise specified
	// TODO find a better handling for the default flags
	// if t.ResetChannels {
	// 	result += "resetchannels"
	// }
	return strings.Trim(result, " ")
}

func (t *RedisACLCommands) String() string {
	result := ""
	ignoreNoCommands := false

	for _, command := range t.Commands {
		result += fmt.Sprintf("+%s ", command)
	}
	if t.AllCommands {
		result += "allcommands "
	}
	if t.NoCommands {
		result += "nocommands"
	}

	//  the string version will ingore '-@all' in case there
	//  is some specific commands for this user. this is because redis will
	//  automatically set '-@all' in order to block
	//  all other commands except the desired onces
	if len(t.Commands) > 0 && (find(t.RmCommands, "@all")) {
		ignoreNoCommands = true
	}

	for _, rmcommand := range t.RmCommands {
		if rmcommand == "@all" && ignoreNoCommands {
			continue
		}

		result += fmt.Sprintf("-%s ", rmcommand)
	}

	return strings.Trim(result, " ")
}

func (t *RedisACLKeys) String() string {
	result := ""
	for _, pattern := range t.Patterns {
		result += fmt.Sprintf("~%s ", pattern)
	}
	if t.AllKeys {
		result += "allchannels "
	}
	if t.ResetKeys {
		result += "resetkeys"
	}
	return strings.Trim(result, " ")
}

func (t *RedisACLPasswords) String() string {
	var passes, rmpasses, hashes, rmhashes, nopass, resetpass string

	if t.NoPass {
		nopass = "nopass "
	}
	if t.ResetPass {
		resetpass = "resetpass "
	}
	for _, rmhash := range t.RmHashes {
		rmhashes += fmt.Sprintf("!%s ", rmhash)
	}
	for _, hash := range t.Hashes {
		hashes += fmt.Sprintf("#%s ", hash)
	}
	return strings.Trim(fmt.Sprintf("%s%s%s%s%s%s", rmpasses, rmhashes, passes, hashes, nopass, resetpass), " ")
}

func (r *RedisACL) String() string {
	result := ""
	for _, user := range r.Users {
		result = fmt.Sprintf("%suser %s", result, user.Name)
		on := "on"
		if !user.On {
			on = "off"
		}
		result = fmt.Sprintf("%s %s", result, on)
		if user.Passwords.String() != "" {
			result = fmt.Sprintf("%s %s", result, user.Passwords.String())
		}
		if user.Keys.String() != "" {
			result = fmt.Sprintf("%s %s", result, user.Keys.String())
		}
		if user.Channels.String() != "" {
			result = fmt.Sprintf("%s %s", result, user.Channels.String())
		}
		if user.Commands.String() != "" {
			result = fmt.Sprintf("%s %s", result, user.Commands.String())
		}
		result += "\n"
	}

	return strings.Trim(result, " ")[:len(result)-1]
}

// Find takes a string slice and looks for an element in it
func find(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}
