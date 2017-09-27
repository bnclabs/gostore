package bubt

import "os"
import "fmt"
import "bytes"
import "sync/atomic"
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
	zfile    []string
	readm    io.ReaderAt   // block reader for m-index
	readzs   []io.ReaderAt // block reader for zero or more z-index.
	rw       *flock.RWMutex

	zblocksize int64
	mblocksize int64
	logprefix  string

	view   View
	cursor Cursor
	index  blkindex
	zblock []byte
	mblock []byte
}

// OpenSnapshot from paths. Returned Snapshot is not safe across
// goroutines. Each routines shall OpenSnapshot to get a snapshot handle.
func OpenSnapshot(name, paths []string, mmap bool) (snap *Snapshot) {
	var err error

	snap = &Snapshot{
		name:      name,
		index:     make(blkindex, 0, 256),
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
	snap.readheader()
	snap.rw, err = flock.New(snap.lockfile(filepath.Dir(snap.mfile)))
	if err != nil {
		panic(err)
	}
	snap.rw.RLock()
	return snap
}

func (snap *Snapshot) loadreaders(paths []string, mmap bool) error {
	npaths = []string{}
	for _, path := range paths {
		if fis, err := ioutil.ReadDir(path); err == nil {
			for _, fi := range fis {
				if !fi.IsDir() || filepath.Base(fi.Name()) != name {
					continue
				}
				npaths = append(npaths, filepath.Join(path, name))
			}
		} else {
			panic(fmt.Errorf("ReadDir(%q) : %v", path, err))
		}
	}
	zfiles = []string{}
	for _, path := range npaths {
		if fis, err := ioutil.ReadDir(path); err == nil {
			if strings.Contains(fi.Name(), "bubt-mindex.data") {
				snap.mfile = filepath.Join(path, fi.Name())
			} else if strings.Contains(fi.Name(), "bubt-zindex") {
				zfiles = append(zfiles, filepath.Join(path, fi.Name()))
			}
		} else {
			panic(fmt.Errorf("ReadDir(%q) : %v", path, err))
		}
	}

	if mfile == "" {
		panic(fmt.Errorf("index file not found"))
	}
	snap.readm = openfile(snap.mfile, true)

	snap.readzs = make([]io.ReaderAt, len(zfiles))
	snap.zfiles = make([]string, len(zfiles))
	for _, zfile := range zfiles {
		x := strings.TrimLeft(filepath.Base(zfile), "zindex")
		x = strings.TrimRight(x, "data")
		shard, _ := AtoI(x[1 : len(x)-1])
		snap.readzs[shard-1] = openfile(fi.Name(), zmap)
		snap.zfiles[shard-1] = zfile
	}
}

func (snap *Snapshot) readheader(r io.ReaderAt) *Snapshot {
	// validate marker block
	fpos := filesize(r) - MarkerBlocksize
	buffer := snap.readat(lib.Fixbuffer(nil, MarkerBlocksize), r, fpos)
	for _, c := range buffer {
		if c != MarkerByte {
			panic("invalid marker block")
		}
	}

	// read metadata blocks
	var scratch [8]byte
	fpos := filesize(r) - MarkerBlocksize - 8
	buffer := snap.readat(scratch[:], r, fpos)
	mdlen := binary.BigEndian.Uint64(buffer)
	fpos -= mdlen - 8
	snap.metadata = snap.readat(lib.Fixbuffer(nil, ln), r, fpos)

	// read settings
	var setts s.Settings
	fpos -= MarkerBlocksize
	data := snap.readat(lib.Fixbuffer(nil, MarkerBlocksize), r, fpos)
	json.Unmarshal(data, &setts)
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

func (snap *Snapshot) readat(buffer []byte, r io.ReaderAt, fpos int64) []byte {
	if n, err := r.ReadAt(buffer, fpos); err != nil {
		panic(err)
	} else if n != len(buffer) {
		panic("readat(%v) %v != %v", fpos, n, len(buffer))
	}
	return buffer
}

func (snap *Snapshot) ID() string {
	return snap.name
}

func (snap *Snapshot) Count() int64 {
	return snap.n_count
}

func (snap *Snapshot) Close() {
	if err := closereadat(readm); err != nil {
		log.Errorf("%v close %q: %v", snap.logprefix, snap.mfile, err)
	}
	for i, rd := range readzs {
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

	snap.mblock = lib.Fixbuffer(snap.mblock, snap.zblocksize)
	m := msnap(snap.readat(snap.mblock, snap.readm, snap.root))
	snap.index = m.getindex(snap.index[:0])
	level, fpos := m.findkey(0, snap.index, key)
	for level == 0 {
		m = msnap(snap.readat(snap.mblock, snap.readm, fpos))
		snap.index = m.getindex(snap.index[:0])
		level, fpos = m.findkey(0, snap.index, key)
	}

	readz := snap.readzs[level-1]
	z := zsnap(snap.readat(snap.zblock, readz, fpos))
	snap.index = z.getindex(index[:0])
	_, v, cas, deleted, ok = z.findkey(0, snap.index, key)
	if value != nil {
		value = lib.Fixbuffer(value, len(v))
		copy(value, v)
	}

	return value, cas, deleted, ok
}

func (snap *Snapshot) View(id uint64) *View {
	return &view
}

func (snap *Snapshot) abortview(view *View) error {
	return nil
}

func (snap *Snapshot) Scan() api.Iterator {
}

func (snap *Snapshot) lockfile(path string) string {
	return filepath.Join(path, "bubt.lock")
}
