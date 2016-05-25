// +build ignore

package storage

import "os"
import "fmt"

const BubtMaxblocksize = 4 * 1024 * 1024 * 1024 // 4GB
const BubtMinblocksize = 512
const BubtMaxzvalue = 65536
const bubtZpoolSize = 1
const bubtMpoolSize = 8
const bubtBufpoolSize = 256

type Bubtstore struct {
	indexfd  *os.File
	datafd   *os.File
	iterator IndexIterator

	frozen bool

	// builder data
	zpool         chan *bubtzblock
	mpool         chan *bubtmblock
	bufpool       chan []byte
	idxch, datach chan []byte
	iquitch       chan struct{}
	dquitch       chan struct{}
	nodes         []Node
	logprefix     string

	// configuration
	indexfile  string
	datafile   string
	mblocksize int
	zblocksize int
	mreduce    bool

	// statistics
	rootfpos   int64
	mnodes     int64
	znodes     int64
	dcount     int64
	h_zentries *histogramInt64
	h_mentries *histogramInt64
	h_keysize  *histogramInt64
	h_valsize  *histogramInt64
	h_redsize  *histogramInt64
	h_depth    *histogramInt64
}

type bubtblock interface {
	startkey() (kpos int64, key []byte)
	offset() int64
	roffset() int64
}

func NewBubtstore(
	name string, iter IndexIterator, config Config, logg Logger) *Bubtstore {

	var err error
	var ok bool

	f := &Bubtstore{
		iterator:   iter,
		zpool:      make(chan *bubtzblock, bubtZpoolSize),
		mpool:      make(chan *bubtmblock, bubtMpoolSize),
		bufpool:    make(chan []byte, bubtBufpoolSize),
		idxch:      make([]byte, bubtBufpoolSize),
		datach:     make([]byte, bubtBufpoolSize),
		iquitch:    make(chan struct{}),
		dquitch:    make(chan struct{}),
		nodes:      make([]Node, 0),
		h_zentries: newhistorgramInt64(32, 1024, 32),
		h_mentries: newhistorgramInt64(512, 4096, 32),
		h_keysize:  newhistorgramInt64(64, 4096, 32),
		h_valsize:  newhistorgramInt64(64, 10*1024*1024, 32),
		h_redsize:  newhistorgramInt64(64, 10*1024*1024, 32),
		h_depth:    newhistorgramInt64(1, 10, 1),
	}
	f.logprefix = fmt.Sprintf("[BUBT-%s]", name)

	if f.indexfile, err = config.String("indexfile"); err != nil {
		panic(err)
	} else if f.indexfd, err = os.Create(indexfile); err != nil {
		panic(err)
	}

	if f.datafile, err = config.String("datafile"); err != nil {
		panic(err)
	} else if datafile != "" {
		if f.datafd, err = os.Create(datafile); err != nil {
			panic(err)
		}
	}

	if f.zblocksize, err = config.Int64("zblocksize"); err != nil {
		panic(err)
	} else if f.zblocksize > BubtMaxblocksize {
		panic(fmt.Errorf("zblocksize %v > %v", f.zblocksize, BubtMaxblocksize))
	} else if f.zblocksize < BubtMinblocksize {
		panic(fmt.Errorf("zblocksize %v < %v", f.zblocksize, BubtMinblocksize))
	} else if f.mblocksize, err = config.Int64("mblocksize"); err != nil {
		panic(err)
	} else if f.mblocksize > BubtMaxblocksize {
		panic(fmt.Errorf("mblocksize %v > %v", f.mblocksize, BubtMaxblocksize))
	} else if f.mblocksize < BubtMinblocksize {
		panic(fmt.Errorf("mblocksize %v < %v", f.mblocksize, BubtMinblocksize))
	} else if f.mreduce, ok = config.Bool("mreduce"); err != nil {
		panic(err)
	} else if f.hasdatafile() == false && f.mreduce == true {
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

func (f *Bubtstore) Build() {
	if f.frozen == false {
		panic("cannot build a frozen bottoms up btree")
	}

	log.Infof("%v build started ...", f.logprefix)
	var block bubtblock

	// add a new level to the btree.
	prependlevel := func(ms []*bubtmblock, mblock *bubtmblock) []*bubtmblock {
		ln, ms = len(ms), append(ms, nil)
		copy(ms[1:], ms[:ln])
		ms[0] = mblock
		return ms
	}

	ms, fpos := []*bubtmblock{}, [2]int64{0, 0}
	for ms, block, fpos = f.buildm(ms, fpos); block != nil; {
		mblock := f.newm()
		if mblock.insert(block) == false {
			panic("error inserting first entry into mblock")
		}
		ms, block, fpos = f.buildm(prependlevel(ms, mblock), fpos)
	}
	f.frozen = true
	f.rootpos = block.offset()

	finblock := make([]byte, 4096)
	if stats := f.stats2json(); len(stats) > 4096 {
		binary.BigEndian.PutUint16(finblock[:8], len(stats))
		copy(finblock[8:], stats)
		f.writeidx(finblock)
	}
	log.Infof("%v wrote stat block\n", f.logprefix)
	if config := f.config2json(); len(config) > 4096 {
		binary.BigEndian.PutUint16(finblock[:8], len(config))
		copy(finblock[8:], config)
		f.writeidx(finblock)
	}
	log.Infof("%v wrote config block\n", f.logprefix)
	for i := 0; i < len(finblock); i++ {
		finblock[i] = 0xAB
	}
	f.writeidx(finblock)
	log.Infof("%v wrote marker block\n", f.logprefix)
	close(f.datach)
	<-datach
	close(f.idxch)
	<-f.idxch
	log.Infof("%v ... build completed", f.logprefix)
}

func (f *Bubtstore) buildm(ms []*bubtmblock, fpos [2]int64) (
	[]*bubtmblock, bubtblock, [2]int64) {

	var block bubtblock

	if len(ms) == 0 {
		block, fpos = f.buildz(fpos)
		return ms, block, fpos
	}

	mblock := ms[0]
	f.dcount++
	defer func() { f.dcount-- }()

	ms, block, fpos = f.buildm(ms[1:], fpos)
	for ok := mblock.insert(block); ok; {
		if ms, block, fpos = f.buildm(ms[1:], fpos); block != nil {
			ok = mblock.insert(block)
			continue
		}
		break
	}
	_, fpos = f.flush(mblock, fpos)
	if block != nil {
		ms[0] = f.newm()
		if ms[0].insert(block) == false {
			panic("error inserting first entry into mblock")
		}
		return ms, mblock, fpos
	}
	return ms, nil, fpos
}

func (f *Bubtstore) buildz(fpos [2]int64) (bubtblock, [2]int64) {
	var nd Node
	z := f.newz(fpos)

	f.dcount++
	defer func() { f.dcount-- }()
	defer func() { f.h_depth.add(f.dcount) }()

	for nd, ok = f.pop(), z.insert(nd); ok; {
		nd = f.pop()
		ok = z.insert(nd)
	}
	if nd != nil {
		f.push(nd)
	}
	return f.flush(z, fpos)
}

func (f *Bubtstore) flush(block bubtblock, fpos [2]int64) (bubtblock, [2]int64) {
	switch blk := block.(type) {
	case *bubtzblock:
		if len(blk.entries) > 0 {
			f.h_zentries(len(blk.entries))
			blk.finalize()
			blk.rpos = fpos[1] + len(blk.dbuffer)
			reducevalue := blk.reduce()
			f.h_redsize.add(len(reducevalue))
			blk.dbuffer = append(blk.dbuffer, reducevalue...)
			vpos := fpos[1] + len(blk.dbuffer)
			if f.writedata(blk.dbuffer); err != nil {
				panic(err)
			}
			kpos := fpos[0] + len(blk.kbuffer)
			if err := f.writeidx(blk.kbuffer[:f.zblocksize]); err != nil {
				panic(err)
			}
			f.zpool <- blk
			return blk, [2]int64{kpos, vpos}
		}
		return nil, fpos

	case *bubtmblock:
		if len(blk.entries) > 0 {
			f.h_zentries(len(blk.entries))
			blk.finalize()
			blk.fpos, blk.rpos = fpos, fpos[1]+len(blk.dbuffer)
			reducevalue := blk.reduce()
			f.h_redsize.add(len(reducevalue))
			blk.dbuffer = append(blk.dbuffer, reducevalue...)
			vpos := fpos[1] + len(blk.dbuffer)
			if f.writedata(blk.dbuffer); err != nil {
				panic(err)
			}
			kpos := fpos[0], len(blk.kbuffer)
			if err := f.writeidx(blk.kbuffer[:f.mblocksize]); err != nil {
				panic(err)
			}
			f.mpool <- blk
			return blk, [2]int64{kpos, vpos}
		}
		return nil, fpos
	}
	panic("unreachable code")
}

//---- local methods

func (f *Bubtstore) pop() Node {
	if ln := len(f.nodes); ln > 0 {
		nd := f.nodes[ln-1]
		f.nodes = fnodes[:ln-1]
		return nd
	}
	return f.iterator.Next()
}

func (f *Bubtstore) push(nd Node) {
	f.nodes = append(f.nodes, nd)
}

func (f *Bubtstore) hasdatafile() bool {
	return f.datafile != ""
}

func (f *Bubtstore) getbuffer() []byte {
	return <-f.bufpool
}

func (f *Bubtstore) putbuffer(buffer []byte) {
	f.bufpool <- buffer
}

func (f *Bubtstore) mvpos(vpos int64) int64 {
	if (vpos & 0x7) != 0 {
		panic(fmt.Errorf("vpos %v expected to 8-bit aligned", vpos))
	}
	return vpos | 0x1
}

func (f Bubtstore) ismvpos(vpos) (int64, bool) {
	if vpos & 0x1 {
		return vpos & 0xFFFFFFFFFFFFFFF8, true
	}
	return vpos & 0xFFFFFFFFFFFFFFF8, false
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
		"h_zentries": f.h_zentries.fullstats(),
		"h_mentries": f.h_mentries.fullstats(),
		"h_keysize":  f.h_keysize.fullstats(),
		"h_valsize":  f.h_valsize.fullstats(),
		"h_redsize":  f.h_redsize.fullstats(),
		"h_depth":    f.h_depth.fullstats(),
	}
	data, err := json.Marshal(stats)
	if err != nil {
		panic(err)
	}
	return data
}

//---- flusher

func (f *Bubtstore) writeidx(data []byte) error {
	select {
	case f.idxch <- data:
	case <-f.iquitch:
		return fmt.Errorf("data flusher exited")
	}
	return nil
}

func (f *Bubtstore) writedata(data []byte) error {
	if len(data) > 0 {
		select {
		case f.datach <- data:
		case <-f.dquitch:
			return fmt.Errorf("data flusher exited")
		}
		return nil
	}
	return nil
}

func (f *Bubtstore) flusher(fd *os.File, ch chan []byte, quitch chan struct{}) {
	log.Infof("%v starting flusher for %v ...", f.logprefix, fd.Name())
	defer close(quitch)
	for block := range ch {
		n, err := fd.Write(block)
		if err != nil {
			log.Errorf("%v write %v: %v", f.logprefix, fd.Name(), err)
			return
		} else if n != len(block) {
			fmsg := "%v partial write %v: %v<%v)"
			log.Errorf(fmsg, f.logprefix, fd.Name(), n, len(block))
			return
		}
		f.putbuffer(block)
	}
	log.Infof("%v stopping flusher for %v", f.logprefix, fd.Name())
}
