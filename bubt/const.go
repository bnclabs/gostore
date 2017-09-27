package bubt

// Minblock minimum block size for a z-node or m-node.
const Minblock = 512

// Maxblock maximum block size for a z-node or m-node.
const Maxblock = 1024 * 0124 * 1024 * 1024

// MarkerByte to populate Markerblock.
const MarkerByte = 0xAB

const zpoolSize = 1
const mpoolSize = 8
const bufpoolSize = mpoolSize + zpoolSize
