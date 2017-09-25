package bubt

type hindex []uint32

func (hi hindex) footprint() int64 {
	return int64(len(hi) * 4)
}

func (hi hindex) nextfootprint() int64 {
	return int64((len(hi) + 1) * 4)
}

func (hi hindex) length() int64 {
	return int64(len(hi))
}
