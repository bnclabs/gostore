package api

type ScalarClock uint64

// Update implement Clock{} interface.
func (this ScalarClock) Update(msg interface{}) Clock {
	if msg != nil {
		switch val := msg.(type) {
		case int8:
			return this + ScalarClock(val)
		case uint8:
			return this + ScalarClock(val)
		case int16:
			return this + ScalarClock(val)
		case uint16:
			return this + ScalarClock(val)
		case int32:
			return this + ScalarClock(val)
		case uint32:
			return this + ScalarClock(val)
		case int64:
			return this + ScalarClock(val)
		case uint64:
			return this + ScalarClock(val)
		case int:
			return this + ScalarClock(val)
		case uint:
			return this + ScalarClock(val)
		}
	}
	return this
}

// Clone implement Clock{} interface.
func (this ScalarClock) Clone() Clock {
	return this
}

// Less implement Clock{} interface.
func (this ScalarClock) Less(other Clock) bool {
	return this < other.(ScalarClock)
}

// LessEqual implement Clock{} interface.
func (this ScalarClock) LessEqual(other Clock) bool {
	return this <= other.(ScalarClock)
}
