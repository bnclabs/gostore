package bubt

import "os"
import "io"
import "fmt"
import "strings"
import "strconv"
import "io/ioutil"
import "path/filepath"
import "encoding/json"
import "encoding/binary"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/flock"
import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"

// Snapshot manages sorted key,value entries in persisted, immutable btree
// built bottoms up and not updated there after.
type Snapshot struct {
	name     string
	root     int64 // fpos into m-index
	metadata []byte
	mfile    string
	zfiles   []string
	readm    io.ReaderAt   // block reader for m-index
	readzs   []io.ReaderAt // block reader for zero or more z-index.
	rw       *flock.RWMutex

	n_count    int64
	zblocksize int64
	mblocksize int64
	logprefix  string

	viewcache chan *View
	curcache  chan *Cursor
	index     blkindex
	zblock    []byte
	mblock    []byte
}

// OpenSnapshot from paths. Returned Snapshot is not safe across
// goroutines. Each routines shall OpenSnapshot to get a snapshot handle.
func OpenSnapshot(name string, paths []string, mmap bool) (snap *Snapshot) {
	var err error

	snap = &Snapshot{
		name:      name,
		viewcache: make(chan *View, 100),   // TODO: no magic number
		curcache:  make(chan *Cursor, 100), // TODO: no magic number
		index:     make(blkindex, 0, 256),  // TODO: no magic number
		logprefix: fmt.Sprintf("[BUBT-%s]", name),
	}

	defer func() {
		if r := recover(); r != nil {
			log.Errorf("%v: %v", snap.logprefix, r)
			snap.Close()
			snap = nil
		}
	}()

	snap.loadreaders(paths, mmap)
	snap.readheader(snap.readm)
	snap.rw, err = flock.New(snap.lockfile(filepath.Dir(snap.mfile)))
	if err != nil {
		panic(err)
	}
	snap.rw.RLock()
	return snap
}

func (snap *Snapshot) loadreaders(paths []string, mmap bool) {
	npaths := []string{}
	for _, path := range paths {
		if fis, err := ioutil.ReadDir(path); err == nil {
			for _, fi := range fis {
				if !fi.IsDir() || filepath.Base(fi.Name()) != snap.name {
					continue
				}
				npaths = append(npaths, filepath.Join(path, snap.name))
			}
		} else {
			panic(fmt.Errorf("ReadDir(%q) : %v", path, err))
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
			panic(fmt.Errorf("ReadDir(%q) : %v", path, err))
		}
	}

	if snap.mfile == "" {
		panic(fmt.Errorf("index file not found"))
	}
	snap.readm = openfile(snap.mfile, true)

	snap.readzs = make([]io.ReaderAt, len(zfiles))
	snap.zfiles = make([]string, len(zfiles))
	for _, zfile := range zfiles {
		x := strings.TrimLeft(filepath.Base(zfile), "zindex")
		x = strings.TrimRight(x, "data")
		shard, _ := strconv.Atoi(x[1 : len(x)-1])
		snap.readzs[shard-1] = openfile(zfile, mmap)
		snap.zfiles[shard-1] = zfile
	}
}

func (snap *Snapshot) readheader(r io.ReaderAt) *Snapshot {
	// validate marker block
	fpos := filesize(r) - MarkerBlocksize
	buffer := lib.Fixbuffer(nil, MarkerBlocksize)
	n, err := r.ReadAt(buffer, fpos)
	if err != nil {
		panic(err)
	} else if n < len(buffer) {
		panic(fmt.Errorf("markblock read only %v(%v)", n, len(buffer)))
	}
	for _, c := range buffer {
		if c != MarkerByte {
			panic("invalid marker block")
		}
	}

	// read metadata blocks
	var scratch [8]byte
	fpos = filesize(r) - MarkerBlocksize - 8
	n, err = r.ReadAt(scratch[:], fpos)
	if err != nil {
		panic(err)
	} else if n < len(scratch) {
		panic(fmt.Errorf("read only %v(%v)", n, len(scratch)))
	}
	mdlen := binary.BigEndian.Uint64(scratch[:])
	fpos -= int64(mdlen) - 8
	buffer = lib.Fixbuffer(buffer, int64(mdlen))
	n, err = r.ReadAt(buffer, fpos)
	if err != nil {
		panic(err)
	} else if n < len(buffer) {
		panic(fmt.Errorf("metadata read only %v(%v)", n, len(buffer)))
	}

	// read settings
	var setts s.Settings
	fpos -= MarkerBlocksize
	buffer = lib.Fixbuffer(buffer, MarkerBlocksize)
	n, err = r.ReadAt(buffer, fpos)
	if err != nil {
		panic(err)
	} else if n < len(buffer) {
		panic(fmt.Errorf("settings read only %v(%v)", n, len(buffer)))
	}
	json.Unmarshal(buffer, &setts)
	if snap.name != setts.String("name") {
		panic("impossible situation")
	}
	snap.zblocksize = setts.Int64("zblocksize")
	snap.mblocksize = setts.Int64("mblocksize")
	snap.n_count = setts.Int64("n_count")

	// root block
	snap.root = fpos - snap.mblocksize
	return snap
}

func (snap *Snapshot) ID() string {
	return snap.name
}

func (snap *Snapshot) Count() int64 {
	return snap.n_count
}

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
	snap.rw.RUnlock()
}

func (snap *Snapshot) Destroy() {
	snap.rw.Lock()
	if err := os.Remove(snap.mfile); err != nil {
		log.Errorf("%v remove %q: %v", snap.logprefix, snap.mfile, err)
	}
	for _, zfile := range snap.zfiles {
		if err := os.Remove(zfile); err != nil {
			log.Errorf("%v remove %q: %v", snap.logprefix, zfile, err)
		}
	}
	snap.rw.Unlock()

	lockfile := snap.lockfile(filepath.Dir(snap.mfile))
	if err := os.Remove(lockfile); err != nil {
		log.Errorf("%v remove %q: %v", snap.logprefix, lockfile, err)
	}
}

func (snap *Snapshot) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	shardidx, fpos := snap.findinmblock(key)
	_, v, cas, deleted, ok = snap.findinzblock(shardidx, fpos, key)
	if value != nil {
		value = lib.Fixbuffer(value, int64(len(v)))
		copy(value, v)
	}
	return value, cas, deleted, ok
}

func (snap *Snapshot) findinmblock(key []byte) (shardidx byte, fpos int64) {
	snap.mblock = lib.Fixbuffer(snap.mblock, snap.mblocksize)
	n, err := snap.readm.ReadAt(snap.mblock, snap.root)
	if err != nil {
		panic(err)
	} else if n < len(snap.mblock) {
		panic(fmt.Errorf("mblock read only %v(%v)", n, (snap.mblock)))
	}
	m := msnap(snap.mblock)
	snap.index = m.getindex(snap.index[:0])
	shardidx, fpos = m.findkey(0, snap.index, key)
	for shardidx == 0 {
		n, err = snap.readm.ReadAt(snap.mblock, fpos)
		if err != nil {
			panic(err)
		} else if n < len(snap.mblock) {
			panic(fmt.Errorf("mblock read only %v(%v)", n, len(snap.mblock)))
		}
		m = msnap(snap.mblock)
		snap.index = m.getindex(snap.index[:0])
		shardidx, fpos = m.findkey(0, snap.index, key)
	}
	return shardidx - 1, fpos
}

func (snap *Snapshot) findinzblock(
	shardidx byte, fpos int64,
	key []byte) (index int, value []byte, cas uint64, deleted, ok bool) {

	snap.zblock = lib.Fixbuffer(snap.zblock, snap.zblocksize)
	readz := snap.readzs[shardidx]
	n, err := readz.ReadAt(snap.zblock, fpos)
	if err != nil {
		panic(err)
	} else if n < len(snap.zblock) {
		panic(fmt.Errorf("zblock read only %v(%v)", n, snap.zblock))
	}
	z := zsnap(snap.zblock)
	snap.index = z.getindex(snap.index[:0])
	index, value, cas, deleted, ok = z.findkey(0, snap.index, key)
	return
}

func (snap *Snapshot) View(id uint64) (view *View) {
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

func (snap *Snapshot) Scan() api.Iterator {
	view := &View{}
	view.id, view.snap, view.cursors = 1, snap, view.cursors[:0]
	cur := view.OpenCursor(nil)
	return cur.YNext
}

func (snap *Snapshot) lockfile(path string) string {
	return filepath.Join(path, "bubt.lock")
}
