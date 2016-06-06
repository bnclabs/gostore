package bubt

import "fmt"
import "os"
import "encoding/binary"

import "github.com/prataprc/storage.go/api"

func OpenBubtstore(name, indexfile, datafile string, zblocksize int64) (f *Bubtstore, err error) {
	f = &Bubtstore{
		name:       name,
		indexfile:  indexfile,
		zblocksize: zblocksize,
	}

	block := make([]byte, markerBlocksize)
	f.logprefix = fmt.Sprintf("[BUBT-%s]", name)

	if f.indexfd, err = os.Open(f.indexfile); err != nil {
		panic(err)
	}
	fi, err := f.indexfd.Stat()
	if err != nil {
		panic(err)
	}
	eof := fi.Size()

	markerat := eof - markerBlocksize
	n, err := f.indexfd.ReadAt(block, markerat)
	if err != nil {
		panic(err)
	} else if int64(n) != markerBlocksize {
		panic("%v partial read: %v != %v", f.logprefix, n, markerBlocksize)
	} else {
		for _, byt := range block {
			if byt != 0xAB { // TODO: not magic numbers
				panic("invalid marker")
			}
		}
	}

	// load config block
	configat := markerat - markerBlocksize
	n, err = f.indexfd.ReadAt(block, configat)
	if err != nil {
		panic(err)
	} else if int64(n) != markerBlocksize {
		panic("%v partial read: %v != %v", f.logprefix, n, markerBlocksize)
	} else {
		f.rootblock = int64(binary.BigEndian.Uint64(block[:8]))
		f.rootreduce = int64(binary.BigEndian.Uint64(block[8:16]))
		ln := binary.BigEndian.Uint16(block[16:18])
		if err := f.json2config(block[18 : 18+ln]); err != nil {
			panic(err)
		}
	}
	// validate config block
	if f.name != name {
		panic(fmt.Errorf("expected name %v, got %v", f.name, name))
	} else if f.zblocksize != zblocksize {
		fmsg := "expected zblocksize %v, got %v"
		panic(fmt.Errorf(fmsg, f.zblocksize, zblocksize))
	}

	// load stats block
	statat := configat - zblocksize
	n, err = f.indexfd.ReadAt(block, statat)
	if err != nil {
		panic(err)
	} else if int64(n) != zblocksize {
		panic("%v partial read: %v != %v", f.logprefix, n, zblocksize)
	} else {
		ln := binary.BigEndian.Uint16(block[:2])
		if err := f.json2stats(block[2 : 2+ln]); err != nil {
			panic(err)
		}
	}

	f.znodepool = make(chan []byte, zpoolSize)
	for i := 0; i < cap(f.znodepool); i++ {
		f.znodepool <- make([]byte, f.zblocksize)
	}
	f.mnodepool = make(chan []byte, mpoolSize)
	for i := 0; i < cap(f.mnodepool); i++ {
		f.mnodepool <- make([]byte, f.mblocksize)
	}

	return f, nil
}

func (f *Bubtstore) rangekey(key []byte, fpos int64, callb api.RangeCallb) bool {
	switch ndblk := f.readat(fpos).(type) {
	case mnode:
		entries := ndblk.entryslice()
		from := ndblk.searchkey(key, entries)
		for x := from; x < uint32(len(entries)/4); x++ {
			if f.rangekey(key, ndblk.getentry(x).vpos(), callb) == false {
				return false
			}
		}
		f.mnodepool <- []byte(ndblk)

	case znode:
		var nd node
		entries := ndblk.entryslice()
		from := ndblk.searchkey(key, entries)
		for x := from; x < uint32(len(entries)/4); x++ {
			koff := x * 4
			offset := fpos + int64(binary.BigEndian.Uint32(entries[koff:koff+4]))
			f.newznode(&nd, []byte(ndblk), offset)
			if callb(&nd) == false {
				return false
			}
		}
		f.znodepool <- []byte(ndblk)
	}
	return true
}

func (f *Bubtstore) readat(fpos int64) (nd interface{}) {
	var data []byte
	if vpos, mok := f.ismvpos(fpos); mok {
		data = <-f.mnodepool
		nd = mnode(data)
	} else {
		data = <-f.znodepool
		nd = znode(data)
	}
	if n, err := f.indexfd.ReadAt(data, fpos); err != nil {
		panic(err)
	} else if n != len(data) {
		panic("partial read")
	}
	return
}
