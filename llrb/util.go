package llrb

import "fmt"

func panicerr(fmsg string, args ...interface{}) {
	panic(fmt.Errorf(fmsg, args...))
}
