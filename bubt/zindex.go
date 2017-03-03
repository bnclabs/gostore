package bubt

type zindex []uint32

func (zi zindex) footprint() int64 {
	return int64(len(zi) * 4)
}

func (zi zindex) nextfootprint() int64 {
	return int64((len(zi) + 1) * 4)
}

func (zi zindex) length() int64 {
	return int64(len(zi))
}
