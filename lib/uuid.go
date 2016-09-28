package lib

import "crypto/rand"

// Uuid bytes of values read from cyptro/rand.
type Uuid []byte

// Newuuid populate buf with unique set of bytes.
func Newuuid(buf Uuid) (Uuid, error) {
	if ln := len(buf); (ln%2) != 0 && ln < 8 {
		return nil, ErrorUuidInvalidSize
	} else if _, err := rand.Read([]byte(buf)); err != nil {
		return nil, err
	}
	return buf, nil
}

// Allocuuid like Newuuid but allocates a new set of bytes, instead of user
// supplied.
func Allocuuid(size int) (Uuid, error) {
	if (size % 2) != 0 {
		return nil, ErrorUuidInvalidSize
	}
	return Newuuid(make([]byte, size))
}

// Format uuid hyphenated string.
func (uuid Uuid) Format(out []byte) int {
	if ln := len(uuid); ln >= 10 {
		n := Uuid(uuid[:4]).format4(out)
		for i := 4; i < ln-6; i += 2 {
			n += Uuid(uuid[i : i+2]).format2(out[n:])
		}
		n += Uuid(uuid[ln-6:]).format6(out[n:])
		return n

	} else if ln == 8 {
		n := Uuid(uuid[:2]).format2(out)
		n += Uuid(uuid[2:8]).format6(out[n:])
		return n

	} else {
		n := 0
		for i := 0; i < ln; i += 2 {
			n += Uuid(uuid[i : i+2]).format2(out[n:])
		}
		return n
	}
	panic("unreachable path")
}

var hexlookup = [16]byte{
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
	'a', 'b', 'c', 'd', 'e', 'f',
}

func (uuid Uuid) format6(out []byte) int {
	n := Uuid(uuid[:4]).format4(out)
	n += Uuid(uuid[4:6]).format2(out[n:])
	return n
}

func (uuid Uuid) format4(out []byte) int {
	n := Uuid(uuid[:2]).format2(out)
	n += Uuid(uuid[2:4]).format2(out[n:])
	return n
}

func (uuid Uuid) format2(out []byte) int {
	out[0] = hexlookup[uuid[0]>>4]
	out[1] = hexlookup[uuid[0]&0xF]
	out[2] = hexlookup[uuid[1]>>4]
	out[3] = hexlookup[uuid[1]&0xF]
	return 4
}
