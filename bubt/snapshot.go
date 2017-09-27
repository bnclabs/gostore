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
	name     string
	root     int64 // fpos into m-index
	metadata []byte
	mfile    string
	zfile    []string
	readm    io.ReaderAt   // block reader for m-index
	readzs   []io.ReaderAt // block reader for zero or more z-index.

	zblocksize int64
	mblocksize int64
	logprefix  string

	viewcache   chan *View
	cursorcache chan *Cursor
}

func OpenSnapshot(name, paths []string, mmap bool) (snap *Snapshot) {
	snap = &Snapshot{
		name:        name,
		viewcache:   make(chan *View, 100),   // TODO: no magic.
		cursorcache: make(chan *Cursor, 100), // TODO: no magic.
		logprefix:   fmt.Sprintf("[BUBT-%s]", name),
	}

	defer func() {
		if r := recover(); r != nil {
			log.Errorf("%v: %v", snap.logprefix, r)
		}
		snap = nil
	}()

	snap.loadreaders(paths, mmap)
	snap.readheader()
	return snap
}

func (snap *Snapshot) loadreaders(paths []string, mmap bool) error {
	zfiles = []string{}
	for _, path := range paths {
		if fis, err := ioutil.ReadDir(path); err == nil {
			for _, fi := range fis {
				if !fi.IsDir() || filepath.Base(fi.Name()) != name {
					continue
				}
				if strings.Contains(fi.Name(), "bubt-mindex.data") {
					snap.mfile = fi.Name()
				} else if strings.Contains(fi.Name(), "bubt-zindex") {
					zfiles = append(zfiles, fi.Name())
				}
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
		snap.readzs[shard] = openfile(fi.Name(), zmap)
		snap.zfiles[shard] = zfile
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

func (snap *Snapshot) Dotdump(buffer io.Writer) {
	snap.dumpkeys(snap.rootblock, "")
}

func (snap *Snapshot) Destroy() {
	for snap.dodestroy() == false {
		time.Sleep(100 * time.Second)
	}
}

func (snap *Snapshot) dodestroy() bool {
	// TODO: there should be a global lock to synchronise this call.
	switch x := snap.readm.(type) {
	case *mmap.ReaderAt:
		if err := x.Close(); err != nil {
			log.Errorf("%v closing %q: %v", snap.logprefix, snap.mfile, err)
		}
	case *os.File:
		if err := x.Close(); err != nil {
			log.Errorf("%v closing %q: %v\n", snap.logprefix, snap.mfile, err)
		}
	}
	if err := os.Remove(snap.mfile); err != nil {
		log.Errorf("%v remove %q: %v", snap.logprefix, snap.mfile, err)
	}

	for i, zfile := range snap.zfiles {
		switch x := snap.readzs[i].(type) {
		case *mmap.ReaderAt:
			if err := x.Close(); err != nil {
				log.Errorf("%v closing %q: %v", snap.logprefix, zfile, err)
			}
		case *os.File:
			if err := x.Close(); err != nil {
				log.Errorf("%v closing %q: %v\n", snap.logprefix, zfile, err)
			}
		}
		if err := os.Remove(zfile); err != nil {
			log.Errorf("%v remove %q: %v", snap.logprefix, zfile, err)
		}
	}

	return true
}

func (snap *Snapshot) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	m := msnap(snap.readat(nil, snap.readm, snap.root))
	index := m.getindex([]uint32{})
	level, fpos := m.getkey(0, hindex(index), key)
	for level == 0 {
		m = msnap(snap.readat([]byte(m), snap.readm, fpos))
		index = m.getindex(index[:0])
		level, fpos = m.getkey(0, hindex(index), key)
	}

	z := zsnap(snap.readat(nil, snap.readzs[level-1], fpos))
	index = z.getindex(index[:0])
	_, v, cas, deleted, ok = z.getkey(0, hindex(index), key)
	if value != nil {
		value = lib.Fixbuffer(value, len(v))
		copy(value, v)
	}

	return value, cas, deleted, ok
}

func (snap *Snapshot) View(id uint64) *View {
	return snap.getview(id)
}

func (snap *Snapshot) abortview(view *View) error {
	snap.putview(view)
	return nil
}

func (snap *Snapshot) Scan() api.Iterator {
}

func (snap *Snapshot) getview(id uint64) (view *View) {
	select {
	case view = <-snap.viewcache:
	default:
		view = newview(id, snap, snap.cursors)
	}
	if view.id, view.snapshot = id, snap; view.id == 0 {
		view.id = uint64(snap.root)
	}
	return
}

func (snap *Snapshot) putview(view *View) {
	for _, cur := range view.cursors {
		view.putcursor(cur)
	}
	view.cursors = view.cursors[:0]
	select {
	case snap.viewcache <- view:
	default: // Left for GC
	}
}
