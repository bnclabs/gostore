package bubt

type blkindex []uint32

func (hi blkindex) footprint() int64 {
	return int64((len(hi) + 1) * 4) // includes count prefix.
}

func (hi blkindex) nextfootprint() int64 {
	return int64((len(hi) + 2) * 4) // includes count prefix.
}

func (hi blkindex) length() int64 {
	return int64(len(hi))
}
