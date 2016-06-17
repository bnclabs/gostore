package bubt

import "fmt"

func panicerr(fmsg string, args ...interface{}) {
	panic(fmt.Errorf(fmsg, args...))
}

func makespace(block []byte, off, ln int) []byte {
	for i, j := off+ln-1, ln-1; j >= 0; i, j = i-1, j-1 {
		block[i] = block[j]
	}
	return block
}
