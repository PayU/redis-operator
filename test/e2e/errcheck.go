package e2e

func Check(err error, fn func(string, ...interface{}), msg string) {
	if err != nil {
		fn("%s: %v", msg, err)
	}
}
