package bogn

import s "github.com/prataprc/gosettings"
import "github.com/cloudfoundry/gosigar"

// TODO: consolidate keycapacity and valcapacity into memcapacity under bogn
// settings.

func Defaultsettings() s.Settings {
	_, _, free := getsysmem()
	return s.Settings{
		"memstore":          "mvcc",
		"dgm":               false,
		"workingset":        false,
		"ratio":             .25,
		"period":            100,
		"llrb.keycapacity":  free,
		"llrb.valcapacity":  free,
		"llrb.snapshottick": 4,
		"llrb.allocator":    "flist",
		"bubt.diskpaths":    "/opt/bogn/",
		"bubt.msize":        4096,
		"bubt.zsize":        4096,
		"bubt.mmap":         true,
	}
	// TODO: make sure that keycapacity + valcapacity is less than 256MB.
}

func getsysmem() (total, used, free uint64) {
	mem := sigar.Mem{}
	mem.Get()
	return mem.Total, mem.Used, mem.Free
}
