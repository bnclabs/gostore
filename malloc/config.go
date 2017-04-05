package malloc

import s "github.com/prataprc/gosettings"

// Defaultsettings for arena.
//
// Configurable parameter:
//
// "capacity" (int64, default: 1024 * 1024 * 1024),
//		capacity in bytes that the arena shall manage for
//		node-blocks using one or more pools.
//
// "minblock" (int64, default: <minblock>),
//		minimum size of allocatable chunk.
//
// "maxblock" (int64, default: <maxblock>),
//		maximum size of allocatable chunk.
//
// "pool.capacity" (int64, default: 2 * 1024 * 1024),
//		limit the size of a pool, irrespective of pool's block size.
//
// "maxpools" (int64, default: malloc.Maxpools),
//		maximum number of pools allowed in an arena.
//
// "maxchunks" (int64, default: malloc.Maxchunks),
//		maximum number of chunks allowed in a pool.
//
// "allocator" (string, default: "flist"),
//		allocater algorithm, can be "flist" or "fbit".
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
