package llrb

// 8 byte overhead
type value struct {
	hdr1 uint64 // blksize(8), vsize(32)
}
