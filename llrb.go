package llrb

import "fmt"

const minKeymem = 64
const maxKeymem = 4096

type LLRB struct {
	ar    *memarena
	count int
	size  int
	root  *node
}

func NewLLRB(config map[string]interface{}) *LLRB {
	validateConfig(config)
	minblock := config["keymem.minblock"].(int)
	maxblock := config["keymem.maxblock"].(int)
	numblocks := config["keymem.numblocks"].(int)
	llrb := &LLRB{
		ar: newmemarena(minblock, maxblock, numblocks),
	}
	return llrb
}

func validateConfig(config map[string]interface{}) {
	minblock := config["keymem.minblock"].(int)
	maxblock := config["keymem.maxblock"].(int)
	numblocks := config["keymem.numblocks"].(int)
	if minblock < minKeymem {
		fmsg := "keymem.minblock < %v configuration"
		panic(fmt.Errorf(fmsg, minKeymem))
	} else if maxblock > maxKeymem {
		fmsg := "keymem.maxblock > %v configuration"
		panic(fmt.Errorf(fmsg, maxKeymem))
	} else if numblocks == 0 {
		panic("keymem.numblocks cannot be ZERO")
	}
}
