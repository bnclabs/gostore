package bubt

import "os"
import "io"
import "fmt"
import "regexp"
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
	lockfile string
	readm    io.ReaderAt   // block reader for m-index
	readzs   []io.ReaderAt // block reader for zero or more z-index.
	rw       *flock.RWMutex
	zsizes   []int64

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
func OpenSnapshot(
	name string, paths []string, mmap bool) (snap *Snapshot, err error) {

	snap = &Snapshot{
		name:      name,
		viewcache: make(chan *View, 100),   // TODO: no magic number
		curcache:  make(chan *Cursor, 100), // TODO: no magic number
		index:     make(blkindex, 0, 256),  // TODO: no magic number
		logprefix: fmt.Sprintf("BUBT [%s]", name),
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s", r)
		}
		if err != nil {
			snap.Close()
		}
	}()

	if err = snap.loadreaders(paths, mmap); err != nil {
		return
	}
	if _, err = snap.readheader(snap.readm); err != nil {
		return
	}

	snap.lockfile = filepath.Join(filepath.Dir(snap.mfile), "bubt.lock")
	if snap.rw, err = flock.New(snap.lockfile); err != nil {
		snap.rw = nil
		snap.Close()
		return nil, err
	}
	snap.rw.RLock()

	// initialize the current capacity of zblock-files.
	snap.zsizes = make([]int64, len(snap.readzs))
	for i := range snap.zfiles {
		snap.zsizes[i] = filesize(snap.readzs[i])
	}
	return
}

func (snap *Snapshot) loadreaders(paths []string, mmap bool) error {
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
			return err
		}
	}

	if snap.mfile == "" {
		return fmt.Errorf("bubt.snap.nomindex")
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
		return snap, fmt.Errorf("bubt.snap.nomarker")
	}

	// validate marker block
	fpos := fsize - MarkerBlocksize
	buffer := lib.Fixbuffer(nil, MarkerBlocksize)
	n, err := r.ReadAt(buffer, fpos)
	if err != nil {
		return snap, err
	} else if n < len(buffer) {
		return snap, fmt.Errorf("bubt.snap.partialmarker")
	}
	for _, c := range buffer {
		if c != MarkerByte {
			fmt.Errorf("bubt.snap.invalidmarker")
		}
	}

	// read metadata blocks
	if fpos -= 8; fpos < 0 {
		return snap, fmt.Errorf("bubt.snap.nomdlen")
	}

	var scratch [8]byte
	n, err = r.ReadAt(scratch[:], fpos)
	if err != nil {
		return snap, err
	} else if n < len(scratch) {
		return snap, fmt.Errorf("bubt.snap.partialmdlen")
	}
	mdlen := binary.BigEndian.Uint64(scratch[:])

	if fpos -= int64(mdlen) - 8; fpos < 0 {
		return snap, fmt.Errorf("bubt.snap.nometadata")
	}

	snap.metadata = lib.Fixbuffer(nil, int64(mdlen))
	n, err = r.ReadAt(snap.metadata, fpos)
	if err != nil {
		return snap, err
	} else if n < len(snap.metadata) {
		return snap, fmt.Errorf("bubt.snap.partialmetadata")
	}
	ln := binary.BigEndian.Uint64(snap.metadata)
	snap.metadata = snap.metadata[8 : 8+ln]

	// read settings
	if fpos -= MarkerBlocksize; fpos < 0 {
		return snap, fmt.Errorf("bubt.snap.nosettings")
	}

	setts := s.Settings{}

	buffer = lib.Fixbuffer(buffer, MarkerBlocksize)
	n, err = r.ReadAt(buffer, fpos)
	if err != nil {
		return snap, err
	} else if n < len(buffer) {
		return snap, fmt.Errorf("bubt.snap.partialsettings")
	}
	ln = binary.BigEndian.Uint64(buffer)
	json.Unmarshal(buffer[8:8+ln], &setts)
	if snap.name != setts.String("name") {
		return snap, fmt.Errorf("bubt.snap.invalidsettings")
	}
	snap.zblocksize = setts.Int64("zblocksize")
	snap.mblocksize = setts.Int64("mblocksize")
	snap.n_count = setts.Int64("n_count")

	// root block
	snap.root = fpos - snap.mblocksize
	return snap, nil
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
	if snap.rw != nil {
		snap.rw.RUnlock()
	}
}

func (snap *Snapshot) Destroy() {
	if snap == nil {
		return
	}
	dirs := map[string]bool{}
	if snap.rw != nil {
		snap.rw.Lock()
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
	if err := os.Remove(snap.lockfile); err != nil {
		log.Errorf("%v %v", snap.logprefix, err)
	}
	for dir := range dirs {
		if err := os.Remove(dir); err != nil {
			log.Errorf("%v %v", snap.logprefix, err)
		}
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
		panic(fmt.Errorf("bubt.snap.mblock.partialread"))
	}
	m := msnap(snap.mblock)
	snap.index = m.getindex(snap.index[:0])
	shardidx, fpos = m.findkey(0, snap.index, key)
	for shardidx == 0 {
		n, err = snap.readm.ReadAt(snap.mblock, fpos)
		if err != nil {
			panic(err)
		} else if n < len(snap.mblock) {
			panic(fmt.Errorf("bubt.snap.mblock.partialread"))
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
		panic(fmt.Errorf("bubt.snap.zblock.partialread"))
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
	view.id, view.snap, view.cursors = 0xC0FFEE, snap, view.cursors[:0]
	cur, err := view.OpenCursor(nil)
	if err != nil || cur == nil {
		return nil
	}
	return cur.YNext
}
