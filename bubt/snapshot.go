package bubt

import "os"
import "fmt"
import "bytes"
import "sync"
import "sync/atomic"
import "encoding/json"
import "encoding/binary"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/lib"
import "github.com/prataprc/storage.go/log"

var readmu sync.Mutex
var openstores = make(map[string]*Snapshot)

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
	mblocksize   int64
	zblocksize   int64
	mreduce      bool
	iterpoolsize int64
	level        byte
	hasdatafile  bool
	hasvbuuid    bool
	hasbornseqno bool
	hasdeadseqno bool
}

func OpenBubtstore(name, path string) (ss *Snapshot, err error) {
	var ok bool

	readmu.Lock()
	defer readmu.Unlock()

	if ss, ok = openstores[name]; ok {
		return ss, nil
	}

	indexfile, datafile := mkfilenames(path)
	ss = &Snapshot{
		name:      name,
		path:      path,
		indexfile: indexfile,
		datafile:  datafile,
	}
	defer func() {
		if err != nil {
			ss.destroy()
			ss = nil
		}
	}()

	ss.logprefix = fmt.Sprintf("BUBT [%s]", name)

	// open indexfile
	if _, err = os.Stat(ss.indexfile); os.IsNotExist(err) {
		log.Errorf("%v file %q not present\n", ss.logprefix, ss.indexfile)
		return ss, err
	}
	ss.indexfd, err = os.OpenFile(ss.indexfile, os.O_RDONLY, 0666)
	if err != nil {
		fmsg := "%v indexfile %q (os.O_RDONLY, 0666): %v\n"
		log.Errorf(fmsg, ss.logprefix, ss.datafile, err)
		return ss, err
	}

	var fi os.FileInfo

	fi, err = ss.indexfd.Stat()
	if err != nil {
		fmsg := "%v unable to stat %q: %v\n"
		log.Errorf(fmsg, ss.logprefix, ss.indexfile, err)
		return ss, err
	}
	eof := fi.Size()

	// header
	var header [40]byte
	var statslen, settslen, mdlen int64
	var n int

	headerat := eof - int64(len(header))
	n, err = ss.indexfd.ReadAt(header[:], headerat)
	if err != nil {
		fmsg := "%v reading %q header: %v\n"
		log.Errorf(fmsg, ss.logprefix, ss.indexfile, err)
		return ss, err

	} else if n != len(header) {
		err = fmt.Errorf("partial header read: %v != %v", n, len(header))
		log.Errorf("%v %v\n", ss.logprefix, err)
		return ss, err
	}
	statslen, n = int64(binary.BigEndian.Uint64(header[:])), 8
	settslen, n = int64(binary.BigEndian.Uint64(header[n:])), n+8
	mdlen, n = int64(binary.BigEndian.Uint64(header[n:])), n+8
	ss.rootblock, n = int64(binary.BigEndian.Uint64(header[n:])), n+8
	ss.rootreduce, n = int64(binary.BigEndian.Uint64(header[n:])), n+8

	markerat := headerat - MarkerBlocksize
	if err = ss.validateMarker(markerat, MarkerBlocksize); err != nil {
		return ss, err
	}

	mdat := markerat - mdlen
	if err = ss.loadMetadata(mdat, mdlen); err != nil {
		return ss, err
	}

	settsat := mdat - settslen
	if err = ss.loadSettings(settsat, settslen); err != nil {
		return ss, err
	}

	statsat := settsat - statslen
	if err = ss.loadStats(statsat, statslen); err != nil {
		return ss, err
	}

	// open datafile, if present
	if ss.datafile != "" {
		if _, err = os.Stat(ss.datafile); os.IsNotExist(err) {
			log.Errorf("%v file %q not present\n", ss.logprefix, ss.datafile)
			return ss, err
		}
		ss.datafd, err = os.OpenFile(ss.datafile, os.O_RDONLY, 0666)
		if err != nil {
			fmsg := "%v datafile %q (os.O_RDONLY, 0666): %v\n"
			log.Errorf(fmsg, ss.logprefix, ss.datafile, err)
			return ss, err
		}
	}

	ss.iterpool = make(chan *iterator, ss.iterpoolsize)

	log.Infof("%v opening snapshot ...\n", ss.logprefix)

	openstores[ss.name] = ss
	return ss, nil
}

func (ss *Snapshot) Metadata() []byte {
	return ss.metadata
}

func (ss *Snapshot) Name() string {
	return ss.path
}

// Indexfile return the indexfile backing this snapshot.
func (ss *Snapshot) Indexfile() string {
	return ss.indexfile
}

// Datafile return the datafile backing this snapshot.
func (ss *Snapshot) Datafile() string {
	return ss.datafile
}

//---- Index{} interface.

// ID implement Index{} interface.
func (ss *Snapshot) ID() string {
	return ss.name
}

// Count implement Index{} interface.
func (ss *Snapshot) Count() int64 {
	return ss.n_count
}

// Isactive implement Index{} interface.
func (ss *Snapshot) Isactive() bool {
	return atomic.LoadInt64(&ss.n_snapshots) > 0
}

// RSnapshot implement Index{} interface.
func (ss *Snapshot) RSnapshot(snapch chan api.IndexSnapshot, next bool) error {
	ss.Refer()
	snapch <- ss
	return nil
}

// Setclock implement Index{} interface.
func (ss *Snapshot) Setclock(clock api.Clock) {
	ss.clock = clock
}

// Clone Index{} interface is not supported
func (ss *Snapshot) Clone(name string) api.Index {
	panic("not supported")
}

// Stats implement Index{} interface.
func (ss *Snapshot) Stats() (map[string]interface{}, error) {
	panic("TBD")
}

// Fullstats implement Index{} interface.
func (ss *Snapshot) Fullstats() (map[string]interface{}, error) {
	panic("TBD")
}

// Log implement Index{} interface.
func (ss *Snapshot) Log(involved string, humanize bool) {
	if atomic.LoadInt64(&ss.n_snapshots) == 0 {
		panic("TBD")
	}
	panic("all snapshots released")
}

// Validate implement Index{} interface.
func (ss *Snapshot) Validate() {
	if atomic.LoadInt64(&ss.n_snapshots) == 0 {
		panic("TBD")
	}
	panic("all snapshots released")
}

// Destroy implement Index{} interface.
func (ss *Snapshot) Destroy() error {
	readmu.Lock()
	defer readmu.Unlock()

	if ss == nil {
		return nil
	}
	return ss.destroy()
}

func (ss *Snapshot) destroy() error {
	if atomic.LoadInt64(&ss.n_snapshots) > 0 {
		return api.ErrorActiveSnapshots
	} else if atomic.LoadInt64(&ss.activeiter) > 0 {
		return api.ErrorActiveIterators
	}

	if err := ss.indexfd.Close(); err != nil {
		log.Errorf("%v closing %q: %v\n", ss.logprefix, ss.indexfile, err)
		return err
	}
	if ss.datafd != nil {
		if err := ss.datafd.Close(); err != nil {
			log.Errorf("%v closing %q %v\n", ss.logprefix, ss.datafile, err)
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

	delete(openstores, ss.name)
	return nil
}

//---- IndexSnapshot interface.

// Getclock implement IndexSnapshot{} interface.
func (ss *Snapshot) Getclock() api.Clock {
	return ss.clock
}

// Refer implement IndexSnapshot{} interface.
func (ss *Snapshot) Refer() {
	atomic.AddInt64(&ss.n_snapshots, 1)
}

// Release implement IndexSnapshot{} interface.
func (ss *Snapshot) Release() {
	atomic.AddInt64(&ss.n_snapshots, -1)
}

//---- IndexReader{} interface.

// Has implement IndexReader{} interface.
func (ss *Snapshot) Has(key []byte) bool {
	return ss.Get(key, nil)
}

// Get implement IndexReader{} interface.
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

// Min implement IndexReader{} interface.
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

// Max implement IndexReader{} interface.
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

// Range implement IndexReader{} interface.
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

// Iterate implement IndexReader{} interface.
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

//---- IndexWriter interface{}

// Upsert IndexWriter{} method, will panic if called.
func (ss *Snapshot) Upsert(key, value []byte, callb api.NodeCallb) error {
	panic("IndexWriter.Upsert() not implemented")
}

// Upsert IndexWriter{} method, will panic if called.
func (ss *Snapshot) UpsertCas(key, value []byte, cas uint64, callb api.NodeCallb) error {
	panic("IndexWriter.UpsertCas() not implemented")
}

// DeleteMin IndexWriter{} method, will panic if called.
func (ss *Snapshot) DeleteMin(callb api.NodeCallb) error {
	panic("IndexWriter.DeleteMin() not implemented")
}

// DeleteMax IndexWriter{} method, will panic if called.
func (ss *Snapshot) DeleteMax(callb api.NodeCallb) error {
	panic("IndexWriter.DeleteMax() not implemented")
}

// Delete IndexWriter{} method, will panic if called.
func (ss *Snapshot) Delete(key []byte, callb api.NodeCallb) error {
	panic("IndexWriter.Delete() not implemented")
}

// Mutations IndexWriter{} method, will panic if called.
func (ss *Snapshot) Mutations(_ []*api.MutationCmd, callb api.NodeCallb) error {
	panic("IndexWriter.Mutations() not implemented")
}

//---- helper methods.

func (ss Snapshot) ismvpos(vpos int64) (int64, bool) {
	if (vpos & 0x1) == 1 {
		return int64(uint64(vpos) & 0xFFFFFFFFFFFFFFF8), true
	}
	return int64(uint64(vpos) & 0xFFFFFFFFFFFFFFF8), false
}

func (ss *Snapshot) json2setts(data []byte) error {
	var setts lib.Settings
	if err := json.Unmarshal(data, &setts); err != nil {
		return err
	}
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
	return nil
}

func (ss *Snapshot) json2stats(data []byte) error {
	if err := json.Unmarshal(data, &ss.builderstats); err != nil {
		return err
	}
	ss.n_count = int64(ss.builderstats["n_count"].(float64))
	return nil
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

func (ss *Snapshot) Dumpkeys() {
	ss.dumpkeys(ss.rootblock, "")
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

func (ss *Snapshot) loadMetadata(mdat int64, mdlen int64) error {
	metadata := make([]byte, mdlen)
	n, err := ss.indexfd.ReadAt(ss.metadata, mdat)
	if err != nil {
		log.Errorf("%v settings ReadAt: %v\n", ss.logprefix, err)
		return err
	} else if int64(n) != mdlen {
		err := fmt.Errorf("partial read: %v != %v", n, mdlen)
		log.Errorf("%v %v\n", ss.logprefix, err)
		return err
	}
	ss.metadata = metadata
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
	} else {
		if err = ss.json2setts(block); err != nil {
			err := fmt.Errorf("json2setts: %v", err)
			log.Errorf("%v %v\n", ss.logprefix, err)
			return err
		}
	}
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
	} else {
		if err = ss.json2stats(block); err != nil {
			err := fmt.Errorf("json2stats: %v", err)
			log.Errorf("%v %v\n", ss.logprefix, err)
			return err
		}
	}
	return nil
}
