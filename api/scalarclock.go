package api

type ScalarClock uint64

// Update implement Clock{} interface.
func (this ScalarClock) Update(msg interface{}) Clock {
	if msg != nil {
		if val, ok := msg.(uint64); ok {
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
	return this < other
}

// LessEqual implement Clock{} interface.
func (this ScalarClock) LessEqual(other Clock) bool {
	return this <= other
}
