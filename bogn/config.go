package bogn

import s "github.com/prataprc/gosettings"
import "github.com/prataprc/gostore/llrb"

// Defaultsettings for bogn instances. Applications can get the default
// settings and tune settings parameter for desired behaviour. Default
// settings include llrb.Defaultsettings().
//
// "memstore" (string, default: "llrb")
//		Type of index for in memory storage, can be "llrb" or "mvcc".
//
// "diskstore" (string, default: "bubt")
//		Type of index for in disk storage, can be "bubt".
//
// "durable" (bool, default:false)
//		Persist index on disk.
//
// "dgm" (bool, default:false)
//		Disk-Greater-than-Memory, configure bogn-index whose size won't
//      fit in memory.
//
// "workingset" (bool, default: false)
//      This configuration is valid only when `dgm` is set to true.
//      Set this as true only when a subset of keys in bogn-index will
//      be actived accessed, either for read or write.
//
// "ratio" (floating, default: .25)
//      This configuration is valid only when `dgm` is set to true.
//      Should be a value between 0.5 to 0.1, when the ratio between
//      newer level's memory footprint (or disk footprint) and older
//      level's disk footprint exceeds bogn-ratio, then the two levels
//		shall be compacted.
//
// "period" (int64, default: 100)
//		Time is seconds to periodically persist transient writes onto disk.
//
// "bubt.msize" (int64, default: 4096)
//		BottomsUpBTree, size of intermediate node on disk.
//
// "bubt.zsize" (int64, default: 4096)
//		BottomsUpBTree, size of leaf node on disk.
//
// "bubt.mmap" (bool, default: true)
//		BottomsUpBTree, whether to memory-map leaf node, intermediate
//		nodes are always memory-mapped.
//
// "bubt.diskpaths" (string, default: "/opt/bogn/")
//		BottomsUpBTree, comma separated list of path to persist intermediate
//		nodes and leaf nodes.
//
func Defaultsettings() s.Settings {
	setts := s.Settings{
		"memstore":   "mvcc",
		"diskstore":  "bubt",
		"merge":      true,
		"durable":    true,
		"dgm":        false,
		"workingset": false,
		"ratio":      .25,
		"period":     100,
	}
	switch setts.String("memstore") {
	case "mvcc", "llrb":
		llrbsetts := llrb.Defaultsettings().AddPrefix("llrb.")
		setts = (s.Settings{}).Mixin(setts, llrbsetts)
	}
	switch setts.String("diskstore") {
	case "bubt":
		bubtsetts := s.Settings{
			"bubt.diskpaths": "/opt/bogn/",
			"bubt.msize":     4096,
			"bubt.zsize":     4096,
			"bubt.mmap":      true,
		}
		setts = (s.Settings{}).Mixin(setts, bubtsetts)
	}
	return setts
}
