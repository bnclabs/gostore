package bubt

import "unsafe"
import "encoding/binary"

type vlogentry struct {
	valuelen uint64
	value    unsafe.Pointer
}

var vlogentrysize = int64(unsafe.Sizeof(vlogentry{})) - 8

func (vle *vlogentry) serialize(
	vsize, vlogpos int64,
	value, vlog, zerovbuff []byte) (bool, int64, int64, []byte) {

	var scratch [8]byte

	if vsize <= 0 {
		return false, 0, vlogpos, vlog
	}

	if int64(len(value)) < vsize {
		remain := vsize - (int64(len(vlog)) % vsize)
		if int64(len(value)) > remain {
			vlog = append(vlog, zerovbuff[:remain]...)
			vlogpos += remain
		}
	}
	vlogpos0 := vlogpos
	binary.BigEndian.PutUint64(scratch[:], uint64(len(value)))
	vlog = append(vlog, scratch[:]...)
	vlog = append(vlog, value...)
	vlogpos += int64(len(scratch) + len(value))
	//fmt.Println("addtovalueblock", len(vlog))
	return true, vlogpos0, vlogpos, vlog
}
