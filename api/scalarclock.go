package api

import "strconv"
import "encoding/binary"
import "fmt"

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

// JSONMarshal implement Clock{} interface.
func (this Scalarclock) JSONMarshal(buf []byte) []byte {
	buf = Fixbuffer(buf, 64)
	return strconv.AppendUint(buf[:0], uint64(this), 16)
}

// JSONUnmarshal implement Clock{} interface.
func (this Scalarclock) JSONUnmarshal(data []byte) (Clock, error) {
	sdata := Bytes2str(data)
	clk, err := strconv.ParseUint(sdata, 16, 64)
	if err != nil {
		return nil, err
	}
	return Scalarclock(clk), nil
}

// Marshal implement Clock{} interface.
func (this Scalarclock) Marshal(buf []byte) []byte {
	buf = Fixbuffer(buf, 64)
	binary.BigEndian.PutUint64(buf, uint64(this))
	return buf[:8]
}

// Unmarshal implement Clock{} interface.
func (this Scalarclock) Unmarshal(data []byte) Clock {
	if data == nil || len(data) == 0 {
		return Scalarclock(0)
	}
	return Scalarclock(binary.BigEndian.Uint64(data))
}

func (this Scalarclock) String() string {
	var dst [16]byte
	out := strconv.AppendUint(dst[:0], (uint64(this)), 10)
	return fmt.Sprintf("%s", out)
}
