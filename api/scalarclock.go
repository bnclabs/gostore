package api

type Scalarclock uint64

// Update implement Clock{} interface.
func (this Scalarclock) Update(msg interface{}) Clock {
	if msg != nil {
		switch val := msg.(type) {
		case int8:
			return this + Scalarclock(val)
		case uint8:
			return this + Scalarclock(val)
		case int16:
			return this + Scalarclock(val)
		case uint16:
			return this + Scalarclock(val)
		case int32:
			return this + Scalarclock(val)
		case uint32:
			return this + Scalarclock(val)
		case int64:
			return this + Scalarclock(val)
		case uint64:
			return this + Scalarclock(val)
		case int:
			return this + Scalarclock(val)
		case uint:
			return this + Scalarclock(val)
		}
	}
	return this
}

// Clone implement Clock{} interface.
func (this Scalarclock) Clone() Clock {
	return this
}

// Less implement Clock{} interface.
func (this Scalarclock) Less(other Clock) bool {
	return this < other.(Scalarclock)
}

// LessEqual implement Clock{} interface.
func (this Scalarclock) LessEqual(other Clock) bool {
	return this <= other.(Scalarclock)
}
