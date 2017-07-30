package bubt

import "os"
import "fmt"
import "bytes"
import "sync/atomic"
import "encoding/json"
import "encoding/binary"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"

// Snapshot manages sorted key,value entries in persisted, immutable btree
// built bottoms up and not updated there after.
type Snapshot struct {
	rootblock  int64
	rootreduce int64
	metadata   []byte
	clock      api.Clock

	// statisitcs, need to be 8 byte aligned.
	n_snapshots int64
	n_count     int64
	n_lookups   int64
	n_ranges    int64

	path      string
	indexfile string
	datafile  string
	indexfd   *os.File
	datafd    *os.File
	logprefix string

	// reader data
	builderstats map[string]interface{}
	iterpool     chan *iterator
	activeiter   int64

	// settings, this must be consistent with Bubt{}.
	name         string
	level        byte
	zblocksize   int64
	mblocksize   int64
	mreduce      bool
	hasdatafile  bool
	iterpoolsize int64
	hasvbuuid    bool
	hasbornseqno bool
	hasdeadseqno bool
}

// OpenBubtstore index, since the index is immutable it is returned
// as a snapshot which implements IndexMeta, Index, IndexReader and
// IndexWriter interfaces. If this index is going to be shared
// between multiple go-routines make sure to call RSnapshot() for
// reference counting. Before this index can be destoryed, all its
// snapshots released and iterators closed.
func OpenBubtstore(name, path string) (ss *Snapshot, err error) {
	defer func() {
		if err != nil {
			ss.destroy()
			ss = nil
		}
	}()

	ss = getstore(name)
	if ss != nil {
		return ss, nil
	}

	indexfile, datafile := mkfilenames(path)
	ss = &Snapshot{
		name:      name,
		path:      path,
		indexfile: indexfile,
		datafile:  datafile,
	}
	ss.logprefix = fmt.Sprintf("BUBT [%s]", name)

	// open indexfile
	if _, err = os.Stat(ss.indexfile); os.IsNotExist(err) {
		log.Errorf("%v file %q not present\n", ss.logprefix, ss.indexfile)
		return nil, err
	}
	ss.indexfd, err = os.OpenFile(ss.indexfile, os.O_RDONLY, 0666)
	if err != nil {
		fmsg := "%v indexfile %q (os.O_RDONLY, 0666): %v\n"
		log.Errorf(fmsg, ss.logprefix, ss.datafile, err)
		return nil, err
	}

	var fi os.FileInfo
	if fi, err = ss.indexfd.Stat(); err != nil {
		fmsg := "%v unable to stat %q: %v\n"
		log.Errorf(fmsg, ss.logprefix, ss.indexfile, err)
		return nil, err
	}

	eof := fi.Size()

	var header [5]int64
	if header, err = ss.readheader(eof); err != nil {
		return nil, err
	}
	statslen, settslen, mdlen := header[0], header[1], header[2]
	ss.rootblock, ss.rootreduce = header[3], header[4]
	markerat := eof - int64(len(header)*8) - MarkerBlocksize
	if err = ss.validateMarker(markerat, MarkerBlocksize); err != nil {
		return nil, err
	}
	mdat := markerat - mdlen
	if err = ss.loadMetadata(mdat, mdlen); err != nil {
		return nil, err
	}
	settsat := mdat - settslen
	if err = ss.loadSettings(settsat, settslen); err != nil {
		return nil, err
	}
	statsat := settsat - statslen
	if err = ss.loadStats(statsat, statslen); err != nil {
		return nil, err
	}

	// TODO: validate the rootblock file-position in the header
	// is same as what we have obtained after reading the statistics
	// field

	// open datafile, if present
	if ss.datafile != "" {
		if _, err = os.Stat(ss.datafile); os.IsNotExist(err) {
			log.Errorf("%v file %q not present\n", ss.logprefix, ss.datafile)
			return nil, err
		}
		ss.datafd, err = os.OpenFile(ss.datafile, os.O_RDONLY, 0666)
		if err != nil {
			fmsg := "%v datafile %q (os.O_RDONLY, 0666): %v\n"
			log.Errorf(fmsg, ss.logprefix, ss.datafile, err)
			return nil, err
		}
	}

	ss.iterpool = make(chan *iterator, ss.iterpoolsize)

	log.Infof("%v opening snapshot ...\n", ss.logprefix)

	setstore(name, ss)
	return ss, nil
}

func (ss *Snapshot) readsettings(setts s.Settings) {
	ss.zblocksize = setts.Int64("zblocksize")
	ss.mblocksize = setts.Int64("mblocksize")
	ss.mreduce = setts.Bool("mreduce")
	ss.iterpoolsize = setts.Int64("iterpool.size")
	ss.level = byte(setts.Int64("level"))
	ss.hasdatafile = setts.Bool("datafile")
	if ss.hasdatafile == false {
		ss.datafile = ""
	}
	ss.hasvbuuid = setts.Bool("metadata.vbuuid")
	ss.hasbornseqno = setts.Bool("metadata.bornseqno")
	ss.hasdeadseqno = setts.Bool("metadata.deadseqno")
}

func (ss *Snapshot) readheader(eof int64) (fields [5]int64, err error) {
	var header [40]byte
	var statslen, settslen, mdlen, rootblock, rootreduce int64
	var n int

	headerat := eof - int64(len(header))
	n, err = ss.indexfd.ReadAt(header[:], headerat)
	if err != nil {
		fmsg := "%v reading %q header: %v\n"
		log.Errorf(fmsg, ss.logprefix, ss.indexfile, err)
		return

	} else if n != len(header) {
		err = fmt.Errorf("partial header read: %v != %v", n, len(header))
		log.Errorf("%v %v\n", ss.logprefix, err)
		return
	}
	statslen, n = int64(binary.BigEndian.Uint64(header[:])), 8
	settslen, n = int64(binary.BigEndian.Uint64(header[n:])), n+8
	mdlen, n = int64(binary.BigEndian.Uint64(header[n:])), n+8
	rootblock, n = int64(binary.BigEndian.Uint64(header[n:])), n+8
	rootreduce, n = int64(binary.BigEndian.Uint64(header[n:])), n+8
	fields = [5]int64{statslen, settslen, mdlen, rootblock, rootreduce}
	return fields, nil
}

func (ss *Snapshot) loadMetadata(mdat int64, mdlen int64) error {
	ss.metadata = make([]byte, mdlen)
	n, err := ss.indexfd.ReadAt(ss.metadata, mdat)
	if err != nil {
		log.Errorf("%v settings ReadAt: %v\n", ss.logprefix, err)
		return err
	} else if int64(n) != mdlen {
		err := fmt.Errorf("partial read: %v != %v", n, mdlen)
		log.Errorf("%v %v\n", ss.logprefix, err)
		return err
	}
	return nil
}

func (ss *Snapshot) loadSettings(statsat int64, statslen int64) error {
	block := make([]byte, statslen)
	n, err := ss.indexfd.ReadAt(block, statsat)
	if err != nil {
		log.Errorf("%v settings ReadAt: %v\n", ss.logprefix, err)
		return err
	} else if int64(n) != statslen {
		err := fmt.Errorf("partial read: %v != %v", n, statslen)
		log.Errorf("%v %v\n", ss.logprefix, err)
		return err
	}
	var setts s.Settings
	if err := json.Unmarshal(block, &setts); err != nil {
		err := fmt.Errorf("setts.Unmarhsal(): %v", err)
		log.Errorf("%v %v\n", ss.logprefix, err)
		return err
	}
	ss.readsettings(setts)
	return nil
}

func (ss *Snapshot) loadStats(statsat int64, statslen int64) error {
	block := make([]byte, statslen)
	n, err := ss.indexfd.ReadAt(block, statsat)
	if err != nil {
		log.Errorf("%v settings ReadAt: %v\n", ss.logprefix, err)
		return err
	} else if int64(n) != statslen {
		err := fmt.Errorf("partial read: %v != %v", n, statslen)
		log.Errorf("%v %v\n", ss.logprefix, err)
		return err
	}
	if err := json.Unmarshal(block, &ss.builderstats); err != nil {
		err := fmt.Errorf("json.Unmarshal: %v", err)
		log.Errorf("%v %v\n", ss.logprefix, err)
		return err
	}
	ss.n_count = int64(ss.builderstats["n_count"].(float64))
	return nil
}

//---- exported methods

// Return snapshot name
func (ss *Snapshot) Name() string {
	return ss.name
}

// Indexfile return the indexfile backing this snapshot.
func (ss *Snapshot) Indexfile() string {
	return ss.indexfile
}

// Datafile return the datafile backing this snapshot.
func (ss *Snapshot) Datafile() string {
	return ss.datafile
}

// Dumpkeys in index
func (ss *Snapshot) Dumpkeys() {
	ss.dumpkeys(ss.rootblock, "")
}

//---- api.IndexMeta interface.

// ID implement api.IndexMeta interface.
func (ss *Snapshot) ID() string {
	return ss.name
}

// Count implement api.IndexMeta interface.
func (ss *Snapshot) Count() int64 {
	return ss.n_count
}

// Isactive implement api.IndexMeta interface.
func (ss *Snapshot) Isactive() bool {
	return atomic.LoadInt64(&ss.n_snapshots) > 0
}

// Getclock implement api.IndexMeta interface.
func (ss *Snapshot) Getclock() api.Clock {
	return ss.clock
}

// Metadata implement api.IndexMeta interface. Opaque binary blob
// persisted by builder as per application's call.
func (ss *Snapshot) Metadata() []byte {
	return ss.metadata
}

// Stats implement api.IndexMeta interface. TBD
func (ss *Snapshot) Stats() (map[string]interface{}, error) {
	panic("TBD")
}

// Fullstats implement api.IndexMeta interface. TBD
func (ss *Snapshot) Fullstats() (map[string]interface{}, error) {
	panic("TBD")
}

// Validate implement api.IndexMeta interface.
func (ss *Snapshot) Validate() {
	if atomic.LoadInt64(&ss.n_snapshots) == 0 {
		panic("TBD")
	}
	panic("all snapshots released")
}

// Log implement api.IndexMeta interface.
func (ss *Snapshot) Log(involved string, humanize bool) {
	if atomic.LoadInt64(&ss.n_snapshots) == 0 {
		panic("TBD")
	}
	panic("all snapshots released")
}

//---- api.Index interface

// RSnapshot implement api.Index interface.
func (ss *Snapshot) RSnapshot(snapch chan api.IndexSnapshot, next bool) error {
	ss.Refer()
	snapch <- ss
	return nil
}

// Setclock implement api.Index interface.
func (ss *Snapshot) Setclock(clock api.Clock) {
	ss.clock = clock
}

// Clone api.Index interface is not supported
func (ss *Snapshot) Clone(name string) (api.Index, error) {
	panic("not supported")
}

// Destroy implement Index{} interface.
func (ss *Snapshot) Destroy() error {
	if ss == nil {
		return nil
	}
	return ss.destroy()
}

//---- api.IndexSnapshot interface.

// Refer implement api.IndexSnapshot{} interface.
func (ss *Snapshot) Refer() {
	atomic.AddInt64(&ss.n_snapshots, 1)
}

// Release implement api.IndexSnapshot{} interface.
func (ss *Snapshot) Release() {
	atomic.AddInt64(&ss.n_snapshots, -1)
}

//---- api.IndexReader interface.

// Has implement api.IndexReader interface.
func (ss *Snapshot) Has(key []byte) bool {
	return ss.Get(key, nil)
}

// Get implement api.IndexReader interface.
func (ss *Snapshot) Get(key []byte, callb api.NodeCallb) bool {
	if ss.n_count == 0 {
		if callb != nil {
			callb(ss, 0, nil, nil, api.ErrorKeyMissing)
		}
		return false
	}

	rc := false
	ss.rangeforward(
		key, key, ss.rootblock, [2]int{0, 0},
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err == nil && nd != nil {
				rc = true
			}
			if callb != nil {
				if err != nil {
					callb(ss, 0, nil, nil, err)
				} else if nd == nil {
					callb(ss, 0, nil, nil, api.ErrorKeyMissing)
				} else {
					callb(ss, 0, nd, nd, nil)
				}
			}
			return false
		})
	atomic.AddInt64(&ss.n_lookups, 1)
	return rc
}

// Min implement api.IndexReader interface.
func (ss *Snapshot) Min(callb api.NodeCallb) bool {
	if ss.n_count == 0 {
		if callb != nil {
			callb(ss, 0, nil, nil, api.ErrorKeyMissing)
		}
		return false
	}

	rc := false
	ss.rangeforward(
		nil, nil, ss.rootblock, [2]int{0, 0},
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err == nil && nd != nil {
				rc = true
			}
			if callb != nil {
				if err != nil {
					callb(ss, 0, nil, nil, err)
				} else if nd == nil {
					callb(ss, 0, nil, nil, api.ErrorKeyMissing)
				} else {
					callb(ss, 0, nd, nd, nil)
				}
			}
			return false
		})
	atomic.AddInt64(&ss.n_lookups, 1)
	return rc
}

// Max implement api.IndexReader interface.
func (ss *Snapshot) Max(callb api.NodeCallb) bool {
	if ss.n_count == 0 {
		if callb != nil {
			callb(ss, 0, nil, nil, api.ErrorKeyMissing)
		}
		return false
	}

	rc := false
	ss.rangebackward(
		nil, nil, ss.rootblock, [2]int{0, 0},
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err == nil && nd != nil {
				rc = true
			}
			if callb != nil {
				if err != nil {
					callb(ss, 0, nil, nil, err)
				} else if nd == nil {
					callb(ss, 0, nil, nil, api.ErrorKeyMissing)
				} else {
					callb(ss, 0, nd, nd, nil)
				}
			}
			return false
		})
	atomic.AddInt64(&ss.n_lookups, 1)
	return rc
}

// Range implement api.IndexReader interface.
func (ss *Snapshot) Range(
	lkey, hkey []byte, incl string, reverse bool, callb api.NodeCallb) {

	if ss.rootblock < 0 {
		return
	}
	lkey, hkey = ss.fixrangeargs(lkey, hkey)
	if lkey != nil && hkey != nil && bytes.Compare(lkey, hkey) == 0 {
		if incl == "none" {
			return
		} else if incl == "low" || incl == "high" {
			incl = "both"
		}
	}

	var cmp [2]int

	if reverse {
		switch incl {
		case "low":
			cmp = [2]int{0, -1}
		case "high":
			cmp = [2]int{1, 0}
		case "both":
			cmp = [2]int{0, 0}
		case "none":
			cmp = [2]int{1, -1}
		}
		ss.rangebackward(lkey, hkey, ss.rootblock, cmp, callb)

	} else {
		switch incl {
		case "low":
			cmp = [2]int{0, -1}
		case "high":
			cmp = [2]int{1, 0}
		case "both":
			cmp = [2]int{0, 0}
		case "none":
			cmp = [2]int{1, -1}
		}
		ss.rangeforward(lkey, hkey, ss.rootblock, cmp, callb)
	}
	atomic.AddInt64(&ss.n_lookups, 1)
	return
}

// Iterate implement api.IndexReader interface.
func (ss *Snapshot) Iterate(
	lkey, hkey []byte, incl string, r bool) api.IndexIterator {

	lkey, hkey = ss.fixrangeargs(lkey, hkey)
	if lkey != nil && hkey != nil && bytes.Compare(lkey, hkey) == 0 {
		if incl == "none" {
			return nil
		} else if incl == "low" || incl == "high" {
			incl = "both"
		}
	}

	var iter *iterator
	select {
	case iter = <-ss.iterpool:
	default:
		iter = &iterator{}
	}

	// NOTE: always re-initialize, because we are getting it back from pool.
	iter.tree, iter.snapshot = ss, ss
	iter.nodes, iter.index, iter.limit = iter.nodes[:0], 0, 5
	iter.continuate = false
	iter.incl, iter.reverse = incl, r
	iter.startkey = lib.Fixbuffer(iter.startkey, int64(len(lkey)))
	copy(iter.startkey, lkey)
	iter.endkey = lib.Fixbuffer(iter.endkey, int64(len(hkey)))
	copy(iter.endkey, hkey)
	iter.closed, iter.activeiter = false, &ss.activeiter

	if iter.nodes == nil {
		iter.nodes = make([]api.Node, 0)
	}

	iter.rangefill()
	if r {
		switch iter.incl {
		case "none":
			iter.incl = "high"
		case "low":
			iter.incl = "both"
		}
	} else {
		switch iter.incl {
		case "none":
			iter.incl = "low"
		case "high":
			iter.incl = "both"
		}
	}

	atomic.AddInt64(&ss.n_ranges, 1)
	atomic.AddInt64(&ss.activeiter, 1)
	return iter
}

//---- api.IndexWriter interface{}

// Upsert api.IndexWriter{} method, will panic if called.
func (ss *Snapshot) Upsert(key, value []byte, callb api.NodeCallb) error {
	panic("IndexWriter.Upsert() not implemented")
}

// Upsert api.IndexWriter{} method, will panic if called.
func (ss *Snapshot) UpsertCas(key, value []byte, cas uint64, callb api.NodeCallb) error {
	panic("IndexWriter.UpsertCas() not implemented")
}

// DeleteMin api.IndexWriter{} method, will panic if called.
func (ss *Snapshot) DeleteMin(callb api.NodeCallb) error {
	panic("IndexWriter.DeleteMin() not implemented")
}

// DeleteMax api.IndexWriter{} method, will panic if called.
func (ss *Snapshot) DeleteMax(callb api.NodeCallb) error {
	panic("IndexWriter.DeleteMax() not implemented")
}

// Delete api.IndexWriter{} method, will panic if called.
func (ss *Snapshot) Delete(key []byte, callb api.NodeCallb) error {
	panic("IndexWriter.Delete() not implemented")
}

// Mutations api.IndexWriter{} method, will panic if called.
func (ss *Snapshot) Mutations(_ []*api.MutationCmd, callb api.NodeCallb) error {
	panic("IndexWriter.Mutations() not implemented")
}

//---- local methods

func (ss *Snapshot) destroy() error {
	if atomic.LoadInt64(&ss.n_snapshots) > 0 {
		return api.ErrorActiveSnapshots
	} else if atomic.LoadInt64(&ss.activeiter) > 0 {
		return api.ErrorActiveIterators
	}

	defer delstore(ss.name)

	if ss.indexfd != nil {
		if err := ss.indexfd.Close(); err != nil {
			fmsg := "%v closing indexfile %q: %v\n"
			log.Errorf(fmsg, ss.logprefix, ss.indexfile, err)
			return err
		}
	}
	if err := os.Remove(ss.indexfile); err != nil {
		fmsg := "%v while removing indexfile %q: %v\n"
		log.Errorf(fmsg, ss.logprefix, ss.indexfile, err)
		return err
	} else {
		log.Infof("%v removing indexfile %q\n", ss.logprefix, ss.indexfile)
	}

	if ss.datafd != nil {
		if err := ss.datafd.Close(); err != nil {
			fmsg := "%v closing datafile %q: %v\n"
			log.Errorf(fmsg, ss.logprefix, ss.datafile, err)
			return err
		}
	}
	if ss.datafile != "" {
		if err := os.Remove(ss.datafile); err != nil {
			fmsg := "%v while removing datafile %q: %v\n"
			log.Errorf(fmsg, ss.logprefix, ss.datafile, err)
			return err
		} else {
			log.Infof("%v removing datafile %q\n", ss.logprefix, ss.datafile)
		}
	}

	if err := os.Remove(ss.path); err != nil {
		fmsg := "%v while removing path %q: %v\n"
		log.Errorf(fmsg, ss.logprefix, ss.path, err)
		return err
	} else {
		log.Infof("%v removing path %q\n", ss.logprefix, ss.path)
	}
	return nil
}

func (ss Snapshot) ismvpos(vpos int64) (int64, bool) {
	if (vpos & 0x1) == 1 {
		return int64(uint64(vpos) & 0xFFFFFFFFFFFFFFF8), true
	}
	return int64(uint64(vpos) & 0xFFFFFFFFFFFFFFF8), false
}

func (ss *Snapshot) rangeforward(
	lkey, hkey []byte, fpos int64, cmp [2]int, callb api.NodeCallb) bool {

	switch ndblk := ss.readat(fpos).(type) {
	case mnode:
		rc := ndblk.rangeforward(ss, lkey, hkey, cmp, callb)
		return rc

	case znode:
		rc := ndblk.rangeforward(ss, lkey, hkey, fpos, cmp, callb)
		return rc
	}
	return true
}

func (ss *Snapshot) rangebackward(
	lkey, hkey []byte, fpos int64, cmp [2]int, callb api.NodeCallb) bool {

	switch ndblk := ss.readat(fpos).(type) {
	case mnode:
		rc := ndblk.rangebackward(ss, lkey, hkey, cmp, callb)
		return rc

	case znode:
		rc := ndblk.rangebackward(ss, lkey, hkey, fpos, cmp, callb)
		return rc
	}
	return true
}

func (ss *Snapshot) readat(fpos int64) (nd interface{}) {
	var data []byte
	vpos, mok := ss.ismvpos(fpos)
	if mok {
		data = make([]byte, ss.mblocksize)
		nd = mnode(data)
	} else {
		data = make([]byte, ss.zblocksize)
		nd = znode(data)
	}
	if n, err := ss.indexfd.ReadAt(data, vpos); err != nil {
		panic(fmt.Errorf("ReadAt %q: %v", ss.indexfile, err))
	} else if n != len(data) {
		panic(fmt.Errorf("ReadAt %q : partial read", ss.indexfile))
	}
	return
}

func (ss *Snapshot) dumpkeys(fpos int64, prefix string) {
	switch ndblk := ss.readat(fpos).(type) {
	case mnode:
		ndblk.dumpkeys(ss, prefix)

	case znode:
		ndblk.dumpkeys(ss, prefix)
	}
}

func (ss *Snapshot) fixrangeargs(lk, hk []byte) ([]byte, []byte) {
	l, h := lk, hk
	if len(lk) == 0 {
		l = nil
	}
	if len(hk) == 0 {
		h = nil
	}
	return l, h
}

func (ss *Snapshot) validateMarker(markerat int64, markerlen int64) error {
	block := make([]byte, markerlen)
	n, err := ss.indexfd.ReadAt(block, markerat)
	if err != nil {
		fmsg := "%v reading %q marker-block: %v\n"
		log.Errorf(fmsg, ss.logprefix, ss.indexfile, err)
		return err

	} else if int64(n) != markerlen {
		err = fmt.Errorf("partial read: %v != %v\n", n, markerlen)
		log.Errorf("%v %v\n", ss.logprefix, err)
		return err

	} else {
		for _, byt := range block {
			if byt != 0xAB { // TODO: not magic numbers
				err = fmt.Errorf("invalid marker")
				log.Errorf("%v %v\n", ss.logprefix, err)
				return err
			}
		}
	}
	return nil
}
