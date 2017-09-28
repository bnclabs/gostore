package bubt

// MarkerBlocksize to close snapshot file.
const MarkerBlocksize = 4096

// MarkerByte to populate Markerblock.
const MarkerByte = 0xAB

const zpoolSize = 1
const mpoolSize = 8
const bufpoolSize = mpoolSize + zpoolSize
