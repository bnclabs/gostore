package bubt

import "fmt"
import "os"
import "encoding/binary"
import "encoding/json"

import "github.com/prataprc/storage.go/lib"

func OpenBubtstore(name, indexfile string, zblocksize int64) (f *Bubtstore, err error) {
	block := make([]byte, zblocksize)

	f = &Bubtstore{}
	f.logprefix = fmt.Sprintf("[BUBT-%s]", name)

	if f.indexfd, err = os.Open(f.indexfile); err != nil {
		panic(err)
	}
	fi, err := f.indexfd.Stat()
	if err != nil {
		panic(err)
	}
	eof := fi.Size()

	markerat := eof - zblocksize
	n, err := f.indexfd.ReadAt(block, markerat)
	if err != nil {
		panic(err)
	} else if int64(n) != zblocksize {
		panic("%v partial read: %v != %v", f.logprefix, n, zblocksize)
	} else {
		for _, byt := range block {
			if byt != 0xAB { // TODO: not magic numbers
				panic("invalid marker")
			}
		}
	}

	var config lib.Config

	configat := markerat - zblocksize
	n, err = f.indexfd.ReadAt(block, configat)
	if err != nil {
		panic(err)
	} else if int64(n) != zblocksize {
		panic("%v partial read: %v != %v", f.logprefix, n, zblocksize)
	} else {
		ln := binary.BigEndian.Uint16(block[:2])
		if err := json.Unmarshal(block[2:2+ln], &config); err != nil {
			panic(err)
		}
	}

	var stat map[string]interface{}

	statat := configat - zblocksize
	n, err = f.indexfd.ReadAt(block, statat)
	if err != nil {
		panic(err)
	} else if int64(n) != zblocksize {
		panic("%v partial read: %v != %v", f.logprefix, n, zblocksize)
	} else {
		ln := binary.BigEndian.Uint16(block[:2])
		if err := json.Unmarshal(block[2:2+ln], &stat); err != nil {
			panic(err)
		}
	}
	return f, nil
}
