package malloc

import s "github.com/prataprc/gosettings"

// Alignment minblock and maxblocks should be multiples of Alignment.
const Alignment = int64(8)

// MEMUtilization is the ratio between allocated memory to application
// and useful memory allocated from OS.
const MEMUtilization = float64(0.95)

// Maxarenasize maximum size of a memory arena. Can be used as default for
// settings-parameter `capacity`.
const Maxarenasize = int64(1024 * 1024 * 1024 * 1024)

// Maxpools maximum number of pools allowed in an arena. Can be used as
// default for settings-parameter `maxpools`.
const Maxpools = int64(512)

// Maxchunks maximum number of chunks allowed in a pool. Can be used as
// default for settings-parameter `maxchunks`.
const Maxchunks = int64(65536)

// Malloc configurable parameters and default settings.
//
// "capacity" (int64, default: 1024 * 1024 * 1024)
//		Maximum memory capacity managed by a single arena. This memory
//		is further managed by pools of different size.
//
// "minblock" (int64, default: <minblock>)
//		Minimum size of a chunk.
//
// "maxblock" (int64, default: <maxblock>)
//		Maximum size of a chunk.
//
// "pool.capacity" (int64, default: 2 * 1024 * 1024)
//		Limit the size of a pool, irrespective of pool's block size.
//
// "maxpools" (int64, default: malloc.Maxpools)
//		Maximum number of pools allowed in an arena.
//
// "maxchunks" (int64, default: malloc.Maxchunks)
//		Maximum number of chunks allowed in a pool.
//
// "allocator" (string, default: "flist")
//		Allocater algorithm, can be "flist" or "fbit".
//
func Defaultsettings(minblock, maxblock int64) s.Settings {
	return s.Settings{
		"capacity":      int64(1024 * 1024 * 1024),
		"minblock":      minblock,
		"maxblock":      maxblock,
		"pool.capacity": int64(2 * 1024 * 1024),
		"maxpools":      Maxpools,
		"maxchunks":     Maxchunks,
		"allocator":     "flist",
	}
}
