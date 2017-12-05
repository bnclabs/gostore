package bubt

import "os"
import "io"
import "fmt"
import "regexp"
import "strings"
import "strconv"
import "runtime"
import "io/ioutil"
import "path/filepath"
import "encoding/json"
import "encoding/binary"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/flock"
import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"

// Snapshot to read index entries persisted using Bubt builder. Since
// no writes are allowed on the btree, any number of snapshots can be
// opened for reading.
type Snapshot struct {
	name     string
	root     int64 // fpos into m-index
	metadata []byte
	mfile    string
	zfiles   []string
	lockfile string
	readm    io.ReaderAt   // block reader for m-index
	readzs   []io.ReaderAt // block reader for zero or more z-index.
	rw       *flock.RWMutex
	zsizes   []int64

	n_count    int64
	footprint  int64
	zblocksize int64
	mblocksize int64
	logprefix  string

	viewcache   chan *View
	curcache    chan *Cursor
	readbuffers chan *buffers
}

type buffers struct {
	index  blkindex
	zblock []byte
	mblock []byte
}

// OpenSnapshot from paths.
func OpenSnapshot(
	name string, paths []string, mmap bool) (snap *Snapshot, err error) {

	cachesize := runtime.GOMAXPROCS(-1) * 4
	snap = &Snapshot{
		name:        name,
		viewcache:   make(chan *View, cachesize),
		curcache:    make(chan *Cursor, cachesize),
		readbuffers: make(chan *buffers, cachesize),
		logprefix:   fmt.Sprintf("BUBT [%s]", name),
	}

	defer func() {
		if err != nil {
			snap.Close()
		}
	}()

	if err = snap.loadreaders(name, paths, mmap); err != nil {
		return
	}
	if _, err = snap.readheader(snap.readm); err != nil {
		return
	}

	snap.lockfile = filepath.Join(filepath.Dir(snap.mfile), "bubt.lock")
	if snap.rw, err = flock.New(snap.lockfile); err != nil {
		snap.rw = nil
		log.Errorf("%v flock.New(): %v", snap.logprefix, err)
		return
	}
	snap.rw.RLock()

	// initialize the current capacity of zblock-files and its footprint.
	snap.footprint = filesize(snap.readm)
	snap.zsizes = make([]int64, len(snap.readzs))
	for i := range snap.zfiles {
		zsize := filesize(snap.readzs[i])
		snap.footprint += zsize
		snap.zsizes[i] = zsize
	}
	return
}

// PurgeSnapshot remove disk footprint of this snapshot.
func PurgeSnapshot(name string, paths []string) {
	log.Infof("force purging snapshot %v", name)
	for _, path := range paths {
		dirpath := filepath.Join(path, name)
		if err := os.RemoveAll(dirpath); err != nil {
			log.Errorf("%v", err)
		}
	}
}

func (snap *Snapshot) loadreaders(
	name string, paths []string, mmap bool) error {

	npaths := []string{}
	for _, path := range paths {
		if fis, err := ioutil.ReadDir(path); err == nil {
			for _, fi := range fis {
				if !fi.IsDir() || filepath.Base(fi.Name()) != name {
					continue
				}
				npaths = append(npaths, filepath.Join(path, name))
			}
		} else {
			log.Errorf("%v ReadDir(): %v", snap.logprefix, err)
			return err
		}
	}
	zfiles := []string{}
	for _, path := range npaths {
		if fis, err := ioutil.ReadDir(path); err == nil {
			for _, fi := range fis {
				if strings.Contains(fi.Name(), "bubt-mindex.data") {
					snap.mfile = filepath.Join(path, fi.Name())
				} else if strings.Contains(fi.Name(), "bubt-zindex") {
					zfiles = append(zfiles, filepath.Join(path, fi.Name()))
				}
			}
		} else {
			log.Errorf("%v ReadDir(): %v", snap.logprefix, err)
			return err
		}
	}

	if snap.mfile == "" {
		err := fmt.Errorf("bubt.snap.nomindex")
		log.Errorf("%v %v", snap.logprefix, err)
		return err
	}
	snap.readm = openfile(snap.mfile, true)

	snap.readzs = make([]io.ReaderAt, len(zfiles))
	snap.zfiles = make([]string, len(zfiles))
	for _, zfile := range zfiles {
		re, _ := regexp.Compile("bubt-zindex-([0-9]+).data")
		matches := re.FindStringSubmatch(filepath.Base(zfile))
		shard, _ := strconv.Atoi(matches[1])
		snap.readzs[shard-1] = openfile(zfile, mmap)
		snap.zfiles[shard-1] = zfile
	}
	return nil
}

func (snap *Snapshot) readheader(r io.ReaderAt) (*Snapshot, error) {
	fsize := filesize(r)
	if fsize < 0 {
		err := fmt.Errorf("bubt.snap.nomarker")
		log.Errorf("%v %v", snap.logprefix, err)
		return snap, err
	}

	// validate marker block
	fpos := fsize - MarkerBlocksize
	buffer := lib.Fixbuffer(nil, MarkerBlocksize)
	n, err := r.ReadAt(buffer, fpos)
	if err != nil {
		log.Errorf("%v Read Markerblocksize: %v", snap.logprefix, err)
		return snap, err
	} else if n < len(buffer) {
		err := fmt.Errorf("bubt.snap.partialmarker")
		log.Errorf("%v Read Markerblocksize: %v", snap.logprefix, err)
		return snap, err
	}
	for _, c := range buffer {
		if c != MarkerByte {
			err = fmt.Errorf("bubt.snap.invalidmarker")
			log.Errorf("%v Read Markerblock: %v", snap.logprefix, err)
			return snap, err
		}
	}

	// read metadata blocks
	if fpos -= 8; fpos < 0 {
		err := fmt.Errorf("bubt.snap.nomdlen")
		log.Errorf("%v Read metadatablock: %v", snap.logprefix, err)
		return snap, err
	}

	var scratch [8]byte
	n, err = r.ReadAt(scratch[:], fpos)
	if err != nil {
		log.Errorf("%v Read metadatablock: %v", snap.logprefix, err)
		return snap, err
	} else if n < len(scratch) {
		err := fmt.Errorf("bubt.snap.partialmdlen")
		log.Errorf("%v Read metadatablock: %v", snap.logprefix, err)
		return snap, err
	}
	mdlen := binary.BigEndian.Uint64(scratch[:])

	if fpos -= int64(mdlen) - 8; fpos < 0 {
		err := fmt.Errorf("bubt.snap.nometadata")
		log.Errorf("%v Read metadatablock: %v", snap.logprefix, err)
		return snap, err
	}

	snap.metadata = lib.Fixbuffer(nil, int64(mdlen))
	n, err = r.ReadAt(snap.metadata, fpos)
	if err != nil {
		log.Errorf("%v Read metadatablock: %v", snap.logprefix, err)
		return snap, err
	} else if n < len(snap.metadata) {
		err := fmt.Errorf("bubt.snap.partialmetadata")
		log.Errorf("%v Read metadatablock: %v", snap.logprefix, err)
		return snap, err
	}
	ln := binary.BigEndian.Uint64(snap.metadata)
	snap.metadata = snap.metadata[8 : 8+ln]

	// read settings
	if fpos -= MarkerBlocksize; fpos < 0 {
		err := fmt.Errorf("bubt.snap.nosettings")
		log.Errorf("%v Read settings: %v", snap.logprefix, err)
		return snap, err
	}

	setts := s.Settings{}

	buffer = lib.Fixbuffer(buffer, MarkerBlocksize)
	n, err = r.ReadAt(buffer, fpos)
	if err != nil {
		log.Errorf("%v Read settings: %v", snap.logprefix, err)
		return snap, err
	} else if n < len(buffer) {
		err := fmt.Errorf("bubt.snap.partialsettings")
		log.Errorf("%v Read settings: %v", snap.logprefix, err)
		return snap, err
	}
	ln = binary.BigEndian.Uint64(buffer)
	json.Unmarshal(buffer[8:8+ln], &setts)
	if snap.name != setts.String("name") {
		err := fmt.Errorf("bubt.snap.invalidsettings")
		log.Errorf("%v Read settings: %v", snap.logprefix, err)
		return snap, err
	}
	snap.zblocksize = setts.Int64("zblocksize")
	snap.mblocksize = setts.Int64("mblocksize")
	snap.n_count = setts.Int64("n_count")

	// root block
	snap.root = fpos - snap.mblocksize
	return snap, nil
}

//---- Exported Control methods

// ID of snapshot, same as name argument passed to OpenSnapshot.
func (snap *Snapshot) ID() string {
	return snap.name
}

// Count number of indexed entries.
func (snap *Snapshot) Count() int64 {
	return snap.n_count
}

// Footprint return the size occupied by this instance on disk.
func (snap *Snapshot) Footprint() int64 {
	return snap.footprint
}

// Metadata return metadata blob associated with this snapshot.
func (snap *Snapshot) Metadata() []byte {
	return snap.metadata
}

// Log vital information
func (snap *Snapshot) Log() {
	fmsg := "%v zblock:%v mblock:%v footprint: %v n_count: %v"
	zsize, msize := snap.zblocksize, snap.mblocksize
	footprint, n_count := snap.footprint, snap.n_count
	log.Infof(fmsg, snap.logprefix, zsize, msize, footprint, n_count)
}

// Validate disk snapshot TODO
func (snap *Snapshot) Validate() {
}

// Close snapshot, will release all in-memory resources but will keep
// the disk files. All Opened-Snapshots must be closed before it can
// be destoryed.
func (snap *Snapshot) Close() {
	if err := closereadat(snap.readm); err != nil {
		log.Errorf("%v close %q: %v", snap.logprefix, snap.mfile, err)
	}
	for i, rd := range snap.readzs {
		err := closereadat(rd)
		if err != nil {
			log.Errorf("%v close %q: %v", snap.logprefix, snap.zfiles[i], err)
		}
	}
	if snap.rw != nil {
		snap.rw.RUnlock()
	}
}

// Destroy snapshot will remove disk footprint of the btree. Can be called
// only after Close is called on all OpenSnapshots.
func (snap *Snapshot) Destroy() {
	if snap == nil {
		return
	}
	log.Infof("%v purging disk snapshot", snap.logprefix)
	dirs := map[string]bool{}
	if snap.rw != nil {
		snap.rw.Lock()
		// lock and remove m-file and one or more z-files.
		if err := os.Remove(snap.mfile); err != nil {
			log.Errorf("%v %v", snap.logprefix, err)
		}
		dirs[filepath.Dir(snap.mfile)] = true
		for _, zfile := range snap.zfiles {
			if err := os.Remove(zfile); err != nil {
				log.Errorf("%v %v", snap.logprefix, err)
			}
			dirs[filepath.Dir(zfile)] = true
		}
		snap.rw.Unlock()
	}
	// remove lock file
	if err := os.Remove(snap.lockfile); err != nil {
		log.Errorf("%v %v", snap.logprefix, err)
	}
	// remove directories path/name for each path in paths
	for dir := range dirs {
		if err := os.Remove(dir); err != nil {
			log.Errorf("%v %v", snap.logprefix, err)
		}
	}
}

//---- Exported Read methods

// Get value for key, if value argument is not nil it will be used to
// copy the entry's value. Also returns entry's cas, whether entry is
// marked as deleted by LSM. If ok is false, then key is not found.
func (snap *Snapshot) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	buf := snap.getreadbuffer()
	shardidx, fpos := snap.findinmblock(key, buf)
	_, v, cas, deleted, ok = snap.findinzblock(shardidx, fpos, key, value, buf)
	snap.putreadbuffer(buf)
	return v, cas, deleted, ok
}

func (snap *Snapshot) findinmblock(
	key []byte, buf *buffers) (shardidx byte, fpos int64) {

	mblock := buf.mblock
	n, err := snap.readm.ReadAt(mblock, snap.root)
	if err != nil {
		panic(err)
	} else if n < len(mblock) {
		panic(fmt.Errorf("bubt.snap.mblock.partialread"))
	}
	m, mbindex := msnap(mblock), buf.index[:0]
	mbindex = m.getindex(mbindex[:0])
	shardidx, fpos = m.findkey(0, mbindex, key)
	for shardidx == 0 {
		n, err = snap.readm.ReadAt(mblock, fpos)
		if err != nil {
			panic(err)
		} else if n < len(mblock) {
			panic(fmt.Errorf("bubt.snap.mblock.partialread"))
		}
		m = msnap(mblock)
		mbindex = m.getindex(mbindex[:0])
		shardidx, fpos = m.findkey(0, mbindex, key)
	}
	return shardidx - 1, fpos
}

func (snap *Snapshot) findinzblock(
	shardidx byte, fpos int64, key, v []byte,
	buf *buffers) (index int, value []byte, cas uint64, deleted, ok bool) {

	var val []byte

	zblock := buf.zblock
	readz := snap.readzs[shardidx]
	n, err := readz.ReadAt(zblock, fpos)
	if err != nil {
		panic(err)
	} else if n < len(zblock) {
		panic(fmt.Errorf("bubt.snap.zblock.partialread"))
	}
	z, zbindex := zsnap(zblock), buf.index[:0]
	zbindex = z.getindex(zbindex[:0])
	index, val, cas, deleted, ok = z.findkey(0, zbindex, key)

	if v != nil {
		value = lib.Fixbuffer(v, int64(len(val)))
		copy(value, val)
	}
	return
}

// BeginTxn is not allowed.
func (snap *Snapshot) BeginTxn(id uint64) api.Transactor {
	panic("not allowed")
}

// View start a read only transaction. Any number of views can be created
// on this snapshot provided they are not concurrently accessed.
func (snap *Snapshot) View(id uint64) api.Transactor {
	var view *View

	select {
	case view = <-snap.viewcache:
	default:
		view = &View{}
	}
	if view.cursors == nil {
		view.cursors = make([]*Cursor, 8)
	}
	view.id, view.snap, view.cursors = id, snap, view.cursors[:0]
	return view
}

func (snap *Snapshot) abortview(view *View) error {
	select {
	case snap.viewcache <- view:
	default: // Leave it for GC.
	}
	return nil
}

// Scan return a full table iterator.
func (snap *Snapshot) Scan() api.Iterator {
	view := &View{}
	view.id, view.snap, view.cursors = 0xC0FFEE, snap, view.cursors[:0]
	cur, err := view.OpenCursor(nil)
	if err != nil || cur == nil {
		return nil
	}
	return cur.YNext
}

//---- Exported Write methods

// Set is not allowed.
func (snap *Snapshot) Set(key, value, oldvalue []byte) (ov []byte, cas uint64) {
	panic("not allowed")
}

// SetCAS is not allowed.
func (snap *Snapshot) SetCAS(
	key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error) {
	panic("not allowed")
}

// Delete is not allowed.
func (snap *Snapshot) Delete(key, oldvalue []byte, lsm bool) ([]byte, uint64) {
	panic("not allowed")
}

//---- local methods

func (snap *Snapshot) getreadbuffer() (buf *buffers) {
	select {
	case buf = <-snap.readbuffers:
	default:
		buf = &buffers{
			index:  make(blkindex, 0, 256),
			mblock: make([]byte, snap.mblocksize),
			zblock: make([]byte, snap.zblocksize),
		}
	}
	return buf
}

func (snap *Snapshot) putreadbuffer(buf *buffers) {
	select {
	case snap.readbuffers <- buf:
	default: // Leave it for GC.
	}
}
