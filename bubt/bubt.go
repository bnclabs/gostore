package bubt

import "os"
import "fmt"
import "encoding/json"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/log"
import "github.com/prataprc/storage.go/lib"

const bubtZpoolSize = 1
const bubtMpoolSize = 8
const bubtBufpoolSize = bubtMpoolSize + bubtZpoolSize

// Bubtstore manages sorted key,value entries in persisted, immutable btree
// build bottoms up and not updated there after.
type Bubtstore struct {
	indexfd  *os.File
	datafd   *os.File
	iterator api.IndexIterator

	frozen bool

	// builder data
	zpool         chan *bubtzblock
	mpool         chan *bubtmblock
	bufpool       chan []byte
	idxch, datach chan []byte
	iquitch       chan struct{}
	dquitch       chan struct{}
	nodes         []api.Node
	logprefix     string

	// configuration
	indexfile  string
	datafile   string
	mblocksize int64
	zblocksize int64
	mreduce    bool

	// statistics
	rootfpos   int64
	mnodes     int64
	znodes     int64
	dcount     int64
	a_zentries *lib.AverageInt64
	a_mentries *lib.AverageInt64
	a_keysize  *lib.AverageInt64
	a_valsize  *lib.AverageInt64
	a_redsize  *lib.AverageInt64
	h_depth    *lib.HistogramInt64
}

type bubtblock interface {
	startkey() (kpos int64, key []byte)
	offset() int64
	roffset() int64
}

func NewBubtstore(name string, iter api.IndexIterator, config lib.Config, logg log.Logger) *Bubtstore {
	var err error

	f := &Bubtstore{
		iterator:   iter,
		zpool:      make(chan *bubtzblock, bubtZpoolSize),
		mpool:      make(chan *bubtmblock, bubtMpoolSize),
		bufpool:    make(chan []byte, bubtBufpoolSize),
		idxch:      make(chan []byte, bubtBufpoolSize),
		datach:     make(chan []byte, bubtBufpoolSize),
		iquitch:    make(chan struct{}),
		dquitch:    make(chan struct{}),
		nodes:      make([]api.Node, 0),
		a_zentries: &lib.AverageInt64{},
		a_mentries: &lib.AverageInt64{},
		a_keysize:  &lib.AverageInt64{},
		a_valsize:  &lib.AverageInt64{},
		a_redsize:  &lib.AverageInt64{},
		h_depth:    lib.NewhistorgramInt64(0, bubtMpoolSize, 1),
	}
	f.logprefix = fmt.Sprintf("[BUBT-%s]", name)

	f.indexfile = config.String("indexfile")
	if f.indexfd, err = os.Create(f.indexfile); err != nil {
		panic(err)
	}

	f.datafile = config.String("datafile")
	if f.datafd, err = os.Create(f.datafile); err != nil {
		panic(err)
	}

	maxblock, minblock := int64(4*1024*1024*1024), int64(512) // TODO: avoid magic numbers
	f.zblocksize = config.Int64("zblocksize")
	if f.zblocksize > maxblock {
		log.Errorf("zblocksize %v > %v", f.zblocksize, maxblock)
	} else if f.zblocksize < minblock {
		log.Errorf("zblocksize %v < %v", f.zblocksize, minblock)
	}
	f.mblocksize = config.Int64("mblocksize")
	if f.mblocksize > maxblock {
		log.Errorf("mblocksize %v > %v", f.mblocksize, maxblock)
	} else if f.mblocksize < minblock {
		log.Errorf("mblocksize %v < %v", f.mblocksize, minblock)
	}
	f.mreduce = config.Bool("mreduce")
	if f.hasdatafile() == false && f.mreduce == true {
		panic("cannot mreduce without datafile")
	}

	// initialize buffer pool
	for i := 0; i < cap(f.bufpool); i++ {
		f.bufpool <- make([]byte, f.zblocksize)
	}

	go f.flusher(f.indexfd, f.idxch, f.iquitch)
	if f.hasdatafile() {
		go f.flusher(f.datafd, f.datach, f.dquitch)
	}
	log.Infof("%v started ...", f.logprefix)
	return f
}

//---- helper methods.

func (f *Bubtstore) hasdatafile() bool {
	return f.datafile != ""
}

func (f *Bubtstore) mvpos(vpos int64) int64 {
	if (vpos & 0x7) != 0 {
		panic(fmt.Errorf("vpos %v expected to 8-bit aligned", vpos))
	}
	return vpos | 0x1
}

func (f Bubtstore) ismvpos(vpos int64) (int64, bool) {
	if (vpos & 0x1) == 1 {
		return int64(uint64(vpos) & 0xFFFFFFFFFFFFFFF8), true
	}
	return int64(uint64(vpos) & 0xFFFFFFFFFFFFFFF8), false
}

func (f *Bubtstore) config2json() []byte {
	config := map[string]interface{}{
		"indexfile":  f.indexfile,
		"datafile":   f.datafile,
		"zblocksize": f.zblocksize,
		"mblocksize": f.mblocksize,
		"mreduce":    f.mreduce,
	}
	data, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return data
}

func (f *Bubtstore) stats2json() []byte {
	stats := map[string]interface{}{
		"rootfpos":   f.rootfpos,
		"mnodes":     f.mnodes,
		"znodes":     f.znodes,
		"a_zentries": f.a_zentries.Stats(),
		"a_mentries": f.a_mentries.Stats(),
		"a_keysize":  f.a_keysize.Stats(),
		"a_valsize":  f.a_valsize.Stats(),
		"a_redsize":  f.a_redsize.Stats(),
		"h_depth":    f.h_depth.Fullstats(),
	}
	data, err := json.Marshal(stats)
	if err != nil {
		panic(err)
	}
	return data
}
