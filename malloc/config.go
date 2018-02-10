package malloc

// Alignment of blocks and chunks should be multiples of configured value.
var Alignment = int64(16)

// MEMUtilization is the ratio between allocated memory to application
// and useful memory allocated from OS.
var MEMUtilization = float64(0.95)

// Maxarenasize maximum size of a memory arena. Can be used as default
// capacity for NewArena()
var Maxarenasize = int64(1024 * 1024 * 1024 * 1024)

// Maxpools maximum number of pools allowed in an arena.
var Maxpools = int64(512)

// Maxchunks maximum number of chunks allowed in a pool.
var Maxchunks = int64(20 * 1024)
