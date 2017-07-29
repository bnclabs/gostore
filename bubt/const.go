package bubt

// Minblock minimum block size for a z-node or m-node.
const Minblock = 512

// Maxblock maximum block size for a z-node or m-node.
const Maxblock = 1024 * 0124 * 1024 * 1024

// MarkerByte to populate Markerblock.
const MarkerByte = 0xAB

// MarkerBlocksize size on disk for marker block.
const MarkerBlocksize = 4096

const zpoolSize = 1
const mpoolSize = 8
const bufpoolSize = mpoolSize + zpoolSize
