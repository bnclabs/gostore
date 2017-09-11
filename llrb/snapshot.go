package llrb

type Snapshot struct {
	llrb      *LLRB1
	root      *Llrbnode1
	logprefix string
}

func (snap *Snapshot) Get(key, value []byte) ([]byte, uint64, bool, bool) {
	return nil, 0, false, false
}
