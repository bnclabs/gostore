package malloc

import "fmt"

import s "github.com/prataprc/gosettings"

// Alignment minblock and maxblocks should be multiples of Alignment.
const Alignment = int64(8)

// MEMUtilization is the ratio between allocated memory to application
// and useful memory allocated from OS.
const MEMUtilization = float64(0.95)

// Maxarenasize maximum size of a memory arena. Can be used as default
// capacity for NewArena()
const Maxarenasize = int64(1024 * 1024 * 1024 * 1024)

// Maxpools maximum number of pools allowed in an arena.
const Maxpools = int64(512)

// Maxchunks maximum number of chunks allowed in a pool.
const Maxchunks = int64(65536)

// Malloc configurable parameters and default settings.
//
// "minblock" (int64, default: <minblock>)
//		Minimum size of a chunk.
//
// "maxblock" (int64, default: <maxblock>)
//		Maximum size of a chunk.
//
// "allocator" (string, default: "flist")
//		Allocater algorithm, can be "flist" or "fbit".
func Defaultsettings(minblock, maxblock) s.Settings {
	if minblock > maxblock {
		panic(fmt.Errorf("minblock(%v) > maxblock(%v)", minblock, maxblock))
	}
	return s.Settings{
		"minblock":  minblock,
		"maxblock":  maxblock,
		"allocator": "flist",
	}
}
