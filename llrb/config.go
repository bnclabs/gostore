package llrb

import s "github.com/prataprc/gosettings"
import "github.com/cloudfoundry/gosigar"

// Defaultsettings for llrb instance.
//
// "keycapacity" (int64)
//		Memory capacity required for keys. Default will be
//		(avgkeysize / (avgkeysize+avgvalsize)) * freeRAM
//
// "valcapacity" (int64)
//		Memory capacity required for values. Default will be
//		(avgvalsize / (avgkeysize+avgvalsize)) * freeRAM
//
// "snapshottick" (int64, default: 1)
//		Used only in MVCC, time period in millisecond, for generating
//      read-snapshots.
//
// "allocator" (string, default: "flist")
//		Type of allocator to use.
//
func Defaultsettings() s.Settings {
	ramsize, _, _ := getsysmem()
	setts := s.Settings{
		"keycapacity":  ramsize,
		"valcapacity":  ramsize,
		"snapshottick": 1,
		"allocator":    "flist",
	}
	return setts
}

func getsysmem() (total, used, free uint64) {
	mem := sigar.Mem{}
	mem.Get()
	return mem.Total, mem.Used, mem.Free
}
