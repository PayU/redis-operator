package rediscli

import (
	"fmt"
	"testing"

	"github.com/go-test/deep"
)

const ACL string = `1) "user admin on #713bfda78870bf9d1b261f565286f85e97ee614efe5f0faf7c34e7ca4f65baca ~* &* +@all"
2) "user default on nopass sanitize-payload ~* &* +@all"
3) "user rdcuser on #400f9f96b4a343f4766d29dbe7bee178d7de6e186464d22378214c0232fb38ca &* -@all +replconf +ping +psync"
4) "user testuser on #13d249f2cb4127b40cfa757866850278793f814ded3c587fe5889e889a7a9f6c ~testkey:* &* -@all +get +set"`

var ACLObj RedisACL = RedisACL{
	Users: []RedisACLUser{
		{
			Name: "admin",
			On:   true,
			Passwords: RedisACLPasswords{
				Hashes: []string{"713bfda78870bf9d1b261f565286f85e97ee614efe5f0faf7c34e7ca4f65baca"},
			},
			Keys: RedisACLKeys{
				Patterns: []string{"*"},
			},
			Channels: RedisACLChannels{
				Patterns: []string{"*"},
			},
			Commands: RedisACLCommands{
				Commands: []string{"@all"},
			},
		},
		{
			Name: "default",
			On:   true,
			Passwords: RedisACLPasswords{
				NoPass: true,
			},
			Keys: RedisACLKeys{
				Patterns: []string{"*"},
			},
			Channels: RedisACLChannels{
				Patterns: []string{"*"},
			},
			Commands: RedisACLCommands{
				Commands: []string{"@all"},
			},
		},
		{
			Name: "rdcuser",
			On:   true,
			Passwords: RedisACLPasswords{
				Hashes: []string{"400f9f96b4a343f4766d29dbe7bee178d7de6e186464d22378214c0232fb38ca"},
			},
			Keys: RedisACLKeys{},
			Channels: RedisACLChannels{
				Patterns: []string{"*"},
			},
			Commands: RedisACLCommands{
				Commands:   []string{"replconf", "ping", "psync"},
				RmCommands: []string{"@all"},
			},
		},
		{
			Name: "testuser",
			On:   true,
			Passwords: RedisACLPasswords{
				Hashes: []string{"13d249f2cb4127b40cfa757866850278793f814ded3c587fe5889e889a7a9f6c"},
			},
			Keys: RedisACLKeys{
				Patterns: []string{"testkey:*"},
			},
			Channels: RedisACLChannels{
				Patterns: []string{"*"},
			},
			Commands: RedisACLCommands{
				Commands:   []string{"get", "set"},
				RmCommands: []string{"@all"},
			},
		},
	},
}

func TestNewRedisACL(t *testing.T) {
	aclObj, err := NewRedisACL(ACL)
	if err != nil {
		t.Errorf("Failed to create the ACL object: %v\n", err)
	}
	if diff := deep.Equal(aclObj, ACLObj); diff != nil {
		t.Errorf("Generated ACL representation is incorrect\n--- generated: ---\n%+v\n--- check: ---\n%+v\ndiff:\n%+v", Pprint(aclObj), Pprint(ACLObj), diff)
	}
	fmt.Printf("Testing acl: %v\n", aclObj)
}
