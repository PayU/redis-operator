package rediscli

import (
	"encoding/json"
	"fmt"
)

// PrettyPrint functionality for structs

func Pprint(str interface{}) string {
	res, err := json.MarshalIndent(str, "", " ")
	if err != nil {
		fmt.Printf("Failed to parse the struct %+v\n", str)
		return ""
	}
	return string(res)
}
