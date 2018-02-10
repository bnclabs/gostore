package llrb

import s "github.com/bnclabs/gosettings"
import "github.com/cloudfoundry/gosigar"

// Defaultsettings for llrb instance.
//
// "memcapacity" (int64, default: available free-ram)
//		Memory capacity required for keys / values. Default will be ramsize.
//
// "snapshottick" (int64, default: 4)
//      Used only in MVCC, time period in millisecond, for generating
//      read-snapshots.
//
// "allocator" (string, default: "flist")
//      Type of allocator to use.
//
func Defaultsettings() s.Settings {
	_, _, freeram := getsysmem()
	setts := s.Settings{
		"memcapacity":  freeram,
		"snapshottick": 4,
		"allocator":    "flist",
	}
	return setts
}

func getsysmem() (total, used, free uint64) {
	mem := sigar.Mem{}
	mem.Get()
	return mem.Total, mem.Used, mem.Free
}
