// +build ignore

package storage

type bubtblock interface {
	getfirstkey() (kpos int, key []byte)
	getfirstval() (vpos int, val []byte)
	bytes() []byte
}

type bubtstore struct {
	indexfd    *File
	datafd     *File
	iterator   IndexIterator
	nodes      []Node
	mblockpool chan []byte
	zblockpool chan []byte
	index_fpos int64
	data_fpos  int64

	// configuration
	indexfile  string
	datafile   string
	mblocksize int
	zblocksize int
}

func newbubtstore(
	iterator IndexIterator, config map[string]interface{}) *bubtstore {

	var err error

	f := &bubtstore{}
	if indexfile, ok := config["indexfile"].(string); ok {
		f.indexfile = indexfile
		if f.indexfd, err = os.Create(indexfile); err != nil {
			panic(err)
		}
	}
	if datafile, ok := config["datafile"].(string); ok {
		f.datafile = datafile
		if f.datafd, err = os.Create(datafile); err != nil {
			panic(err)
		}
	}
	if sz, ok := config["zblocksize"].(float64); ok {
		f.zblocksize = int64(sz)
	} else if sz > bubtMaxblocksize {
		panic("please define the z-block size")
	}
	if sz, ok := config["mblocksize"].(float64); ok {
		f.mblocksize = int64(sz)
	} else if sz > bubtMaxblocksize {
		panic("please define the m-block size")
	}
	f.iterator = iterator
	f.nodes = make([]Node, 0)
	f.mblockpool = make(chan []byte, 100)
	f.zblockpool = make(chan []byte, 100)
	return f
}

func (f *bubtstore) getzblock() []byte {
	select {
	case buf := <-f.zblockpool:
		return buf
	default:
		return make([]byte, f.zblocksize)
	}
	panic("unreachable code")
}

func (f *bubtstore) getmblock() []byte {
	select {
	case buf := <-f.mblockpool:
		return buf
	default:
		return make([]byte, f.mblocksize)
	}
	panic("unreachable code")
}

func (f *bubtstore) pop() Node {
	if ln := len(f.nodes); ln > 0 {
		nd := f.nodes[ln-1]
		f.nodes = fnodes[:ln-1]
		return nd
	}
	return f.iterator.Next()
}

func (f *bubtstore) push(nd Node) {
	f.nodes = append(f.nodes, nd)
}

func (f *bubtstore) flush(block bubtblock) bubtblock {
	buffer := block.bytes()
	// TODO: flush the buffer into file.
	switch block.(type) {
	case *bubtmblock:
		f.mblockpool <- buffer
	case *bubtzblock:
		f.zblockpool <- buffer
	}
	return block
}
