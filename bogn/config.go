package bogn

import s "github.com/bnclabs/gosettings"
import "github.com/bnclabs/gostore/llrb"

// Defaultsettings for bogn instances. Applications can get the default
// settings and tune settings parameter for desired behaviour. Default
// settings include llrb.Defaultsettings().
//
// "logpath" (string, default: "")
//		Directory path to store log files. If not supplied, and durable
//		is true, then one of the diskpath from diskstore will be used.
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
// "flushratio" (floating, default: .25)
//      This configuration is valid only when `dgm` is set to true.
//      When ratio between memory footprint, for the latest batch of
//		mutations, and latest level's disk-footprint falls below flushratio,
//		then a newer level will be created to flush the latest batch of
//		mutations in memory.
//
// "flushperiod" (int64, default: 100)
//		Time is seconds to periodically persist transient writes onto disk.
//
// "compactratio" (floating, default: .50)
//      This configuration is valid only when `dgm` is set to true.
//		Two succesive disk levels will be merged together into a single
//		snapshot if their ratio exceed compactratio.
//
// "compactperiod" (int64, default: 3600)
//      If the lifetime, measured in seconds, of a disk snapshot exceeds
//		compactperiod, then it will be merged with next disk level snapshot.
//
// "bubt.mblocksize" (int64, default: 4096)
//		BottomsUpBTree, size of intermediate node, m-nodes, on disk.
//
// "bubt.zblocksize" (int64, default: same as mblocksize)
//		BottomsUpBTree, size of leaf node, z-nodes, on disk.
//
// "bubt.vblocksize" (int64, default: same as mblocksize)
//		BottomsUpBTree, size of value log blocsk, on disk.
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
		"logpath":       "",
		"memstore":      "mvcc",
		"diskstore":     "bubt",
		"durable":       true,
		"dgm":           false,
		"workingset":    false,
		"flushratio":    0.25,
		"flushperiod":   100,
		"compactratio":  0.50,
		"compactperiod": 300,
	}
	switch setts.String("memstore") {
	case "mvcc", "llrb":
		llrbsetts := llrb.Defaultsettings().AddPrefix("llrb.")
		setts = (s.Settings{}).Mixin(setts, llrbsetts)
	}
	switch setts.String("diskstore") {
	case "bubt":
		bubtsetts := s.Settings{
			"bubt.diskpaths":  "/opt/bogn/",
			"bubt.mblocksize": 4096,
			"bubt.zblocksize": 4096,
			"bubt.vblocksize": 4096 * 4,
			"bubt.mmap":       true,
		}
		setts = (s.Settings{}).Mixin(setts, bubtsetts)
	}
	return setts
}
