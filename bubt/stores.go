package bubt

import "sync"

var storemu sync.Mutex
var openstores = make(map[string]*Snapshot)

func setstore(name string, ss *Snapshot) {
	storemu.Lock()
	defer storemu.Unlock()
	openstores[name] = ss
}

func getstore(name string) *Snapshot {
	storemu.Lock()
	defer storemu.Unlock()
	ss, _ := openstores[name]
	return ss
}

func delstore(name string) {
	storemu.Lock()
	defer storemu.Unlock()
	delete(openstores, name)
}
