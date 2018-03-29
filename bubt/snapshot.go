package bubt

import "os"
import "io"
import "fmt"
import "time"
import "bytes"
import "regexp"
import "strings"
import "strconv"
import "runtime"
import "io/ioutil"
import "path/filepath"

import "github.com/bnclabs/gostore/api"
import "github.com/bnclabs/gostore/lib"
import "github.com/bnclabs/gostore/flock"
import s "github.com/bnclabs/gosettings"

// Snapshot to read index entries persisted using Bubt builder. Since
// no writes are allowed on the btree, any number of snapshots can be
// opened for reading.
type Snapshot struct {
	name     string
	root     int64 // fpos into m-index
	metadata []byte
	mfile    string
	zfiles   []string
	vfiles   []string
	lockfile string
	readm    io.ReaderAt   // block reader for m-index
	readzs   []io.ReaderAt // block reader for zero or more z-index.
	readvs   []io.ReaderAt
	rw       *flock.RWMutex
	zsizes   []int64

	// from info block
	zblocksize int64
	mblocksize int64
	vblocksize int64
	buildtime  int64
	epoch      int64
	seqno      int64
	keymem     int64
	valmem     int64
	paddingmem int64
	n_zblocks  int64
	n_mblocks  int64
	n_vblocks  int64
	n_count    int64
	n_deleted  int64
	footprint  int64
	logprefix  string

	viewcache chan *View
	curcache  chan *Cursor
	rdpool    *readerpool
}

// OpenSnapshot from paths.
func OpenSnapshot(
	name string, paths []string, mmap bool) (snap *Snapshot, err error) {

	max := runtime.GOMAXPROCS(-1) * 4
	snap = &Snapshot{
		name:      name,
		viewcache: make(chan *View, max),
		curcache:  make(chan *Cursor, max),
		logprefix: fmt.Sprintf("BUBT [%s]", name),
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
	msize, zsize, vsize := snap.mblocksize, snap.zblocksize, snap.vblocksize
	snap.rdpool = newreaderpool(msize, zsize, vsize, int64(max))

	snap.lockfile = filepath.Join(filepath.Dir(snap.mfile), "bubt.lock")
	if snap.rw, err = flock.New(snap.lockfile); err != nil {
		snap.rw = nil
		errorf("%v flock.New(): %v", snap.logprefix, err)
		return
	}
	snap.rw.RLock()

	// initialize the current capacity of zblock-files and its footprint.
	snap.footprint = filesize(snap.readm)
	snap.zsizes = make([]int64, len(snap.readzs))
	for i := range snap.zfiles {
		zsize := filesize(snap.readzs[i])
		snap.footprint += zsize
		if len(snap.readvs) > 0 {
			snap.footprint += filesize(snap.readvs[i])
		}
		snap.zsizes[i] = zsize
	}
	snap.validatequick()
	return
}

// PurgeSnapshot remove disk footprint of this snapshot.
func PurgeSnapshot(name string, paths []string) {
	infof("force purging snapshot %v", name)
	for _, path := range paths {
		dirpath := filepath.Join(path, name)
		if err := os.RemoveAll(dirpath); err != nil {
			errorf("%v", err)
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
			errorf("%v ReadDir(): %v", snap.logprefix, err)
			return err
		}
	}
	zfiles, vfiles := []string{}, []string{}
	for _, path := range npaths {
		if fis, err := ioutil.ReadDir(path); err == nil {
			for _, fi := range fis {
				if strings.Contains(fi.Name(), "bubt-mindex.data") {
					snap.mfile = filepath.Join(path, fi.Name())
				} else if strings.Contains(fi.Name(), "bubt-zindex") {
					zfiles = append(zfiles, filepath.Join(path, fi.Name()))
				} else if strings.Contains(fi.Name(), "bubt-vlog") {
					vfiles = append(vfiles, filepath.Join(path, fi.Name()))
				}
			}
		} else {
			errorf("%v ReadDir(): %v", snap.logprefix, err)
			return err
		}
	}
	if len(vfiles) > 0 && len(vfiles) != len(zfiles) {
		arg1, arg2 := strings.Join(zfiles, ","), strings.Join(vfiles, ",")
		panic(fmt.Errorf("mismatch zfiles: %v, vfiles: %v", arg1, arg2))
	}

	if snap.mfile == "" {
		err := fmt.Errorf("bubt.snap.nomindex")
		errorf("%v %v", snap.logprefix, err)
		return err
	}
	snap.readm = openfile(snap.mfile, true)

	// open zindex file
	snap.readzs = make([]io.ReaderAt, len(zfiles))
	snap.zfiles = make([]string, len(zfiles))
	for _, zfile := range zfiles {
		re, _ := regexp.Compile("bubt-zindex-([0-9]+).data")
		matches := re.FindStringSubmatch(filepath.Base(zfile))
		zshard, _ := strconv.Atoi(matches[1])
		snap.readzs[zshard-1] = openfile(zfile, mmap)
		snap.zfiles[zshard-1] = zfile
	}

	// open vlog file, if any
	snap.readvs = make([]io.ReaderAt, len(vfiles))
	snap.vfiles = make([]string, len(vfiles))
	for _, vfile := range vfiles {
		re, _ := regexp.Compile("bubt-vlog-([0-9]+).data")
		matches := re.FindStringSubmatch(filepath.Base(vfile))
		vshard, _ := strconv.Atoi(matches[1])
		snap.readvs[vshard-1] = openfile(vfile, mmap)
		snap.vfiles[vshard-1] = vfile
	}

	return nil
}

func (snap *Snapshot) readheader(r io.ReaderAt) (*Snapshot, error) {
	err := readmarker(r)
	if err != nil {
		errorf("%v %v", snap.logprefix, err)
		return snap, err
	}
	snap.metadata, err = readmetadata(r)
	if err != nil {
		errorf("%v %v", snap.logprefix, err)
		return snap, err
	}
	fpos, info, err := readinfoblock(r)
	if err != nil {
		errorf("%v %v", snap.logprefix, err)
		return snap, err
	}
	if snap.name != info.String("name") {
		err := fmt.Errorf("bubt.snap.invalidinfoblock")
		errorf("%v Read infoblock: %v", snap.logprefix, err)
		return snap, err
	}
	snap.zblocksize = info.Int64("zblocksize")
	snap.mblocksize = info.Int64("mblocksize")
	snap.vblocksize = info.Int64("vblocksize")
	snap.buildtime = info.Int64("buildtime")
	snap.epoch = info.Int64("epoch")
	snap.seqno = info.Int64("seqno")
	snap.keymem = info.Int64("keymem")
	snap.valmem = info.Int64("valmem")
	snap.paddingmem = info.Int64("paddingmem")
	snap.n_zblocks = info.Int64("n_zblocks")
	snap.n_mblocks = info.Int64("n_mblocks")
	snap.n_vblocks = info.Int64("n_vblocks")
	snap.n_count = info.Int64("n_count")
	snap.n_deleted = info.Int64("n_deleted")

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

// Info return parameters used to build the snapshot and statistical
// information.
//
//   mfile		: m-index file name.
//   zfiles		: list of z-index file name.
//   vfiles      : list of value log files for each each z-index, if present.
//   zblocksize : block size used for z-index file.
//   mblocksize : block size used for m-index file.
//   vblocksize : block size used for value log.
//   buildtime  : time taken, in nanoseconds, to build this snapshot.
//   epoch      : snapshot born time, in nanosec, after January 1, 1970 UTC.
//   seqno      : maximum seqno contained in this snapshot.
//   keymem     : total payload size for all keys.
//   valmem     : total payload size for all values.
//   paddingmem : total bytes used for padding m-block and z-block alignment.
//   n_zblocks  : total number of blocks in z-index files.
//   n_mblocks  : total number of blocks in m-index files.
//   n_vblocks  : total number of blocks in value log.
//   n_count    : number of entries in this snapshot, includes deleted.
//   n_deleted  : number of entries marked as deleted.
//   footprint  : disk footprint for this snapshot.
func (snap *Snapshot) Info() s.Settings {
	return s.Settings{
		"mfile":      snap.mfile,
		"zfiles":     snap.zfiles,
		"vfiles":     snap.vfiles,
		"zblocksize": snap.zblocksize,
		"mblocksize": snap.mblocksize,
		"vblocksize": snap.vblocksize,
		"buildtime":  snap.buildtime,
		"epoch":      snap.epoch,
		"seqno":      snap.seqno,
		"keymem":     snap.keymem,
		"valmem":     snap.valmem,
		"paddingmem": snap.paddingmem,
		"n_zblocks":  snap.n_zblocks,
		"n_mblocks":  snap.n_mblocks,
		"n_vblocks":  snap.n_vblocks,
		"n_count":    snap.n_count,
		"n_deleted":  snap.n_deleted,
		"footprint":  snap.footprint,
	}
}

// Log vital information
func (snap *Snapshot) Log() {
	info := snap.Info()

	// seqno
	fmsg := "%v has %v entries, %v deleted, with maximum seqno %v"
	n_count := info.Int64("n_count")
	n_deleted := info.Int64("n_deleted")
	seqno := info.Int64("seqno")
	infof(fmsg, snap.logprefix, n_count, n_deleted, seqno)

	// log files
	infof("%v m-index file: %q", snap.logprefix, info.String("mfile"))
	zfiles, vfiles := info.Strings("zfiles"), info.Strings("vfiles")
	for i, zfile := range zfiles {
		infof("%v z-index file: %q", snap.logprefix, zfile)
		if len(vfiles) > 0 {
			infof("%v value-log file: %q", snap.logprefix, vfiles[i])
		}
	}

	fmsg = "%v m-block:%v z-block:%v v-block: %v"
	zsize, msize := info.Int64("zblocksize"), info.Int64("mblocksize")
	vsize := info.Int64("vblocksize")
	infof(fmsg, snap.logprefix, zsize, msize, vsize)

	fmsg = "%v built at %v, took %v to build -- {m:%v, z:%v, v:%v}"
	epoch := time.Unix(info.Int64("epoch"), 0)
	took := time.Duration(info.Int64("buildtime")).Round(time.Second)
	mblocks, zblocks := info.Int64("n_mblocks"), info.Int64("n_zblocks")
	vblocks := info.Int64("n_vblocks")
	infof(fmsg, snap.logprefix, epoch, took, mblocks, zblocks, vblocks)

	fmsg = "%v disk footprint is %v for a payload of %v ratio: %.2f"
	payload := info.Int64("keymem") + info.Int64("valmem")
	footprint := info.Int64("footprint")
	ratio := float64(payload) / float64(footprint)
	infof(fmsg, snap.logprefix, footprint, payload, ratio)
}

// Validate snapshot on disk. This is a costly call, use it only
// for testing and administration purpose.
func (snap *Snapshot) Validate() {
	var keymem, valmem, n_count, n_deleted int64
	var maxseqno uint64
	var prevkey []byte

	iter := snap.Scan()
	key, val, seqno, del, err := iter(false /*fin*/)
	for err == nil {
		keymem = keymem + int64(len(key))
		if seqno > maxseqno {
			maxseqno = seqno
		}
		if del {
			n_deleted++
		} else {
			valmem += int64(len(val))
		}
		if len(prevkey) > 0 {
			if bytes.Compare(prevkey, key) >= 0 {
				panic(fmt.Errorf("key %q comes before %q", prevkey, key))
			}
		}
		n_count++
		prevkey = lib.Fixbuffer(prevkey, int64(len(key)))
		copy(prevkey, key)
		key, val, seqno, del, err = iter(false /*fin*/)
	}
	iter(true /*fin*/)

	// validate count
	if n_count != snap.n_count {
		fmsg := "expected %v entries, found %v"
		panic(fmt.Errorf(fmsg, snap.n_count, n_count))
	}
	// validate keymem, valmem
	if keymem != snap.keymem {
		fmsg := "build time keymem %v != actual %v"
		panic(fmt.Errorf(fmsg, snap.keymem, keymem))
	}
	if valmem != snap.valmem {
		fmsg := "build time valmem %v != actual %v"
		panic(fmt.Errorf(fmsg, snap.valmem, valmem))
	}

	snap.validatequick()
}

func (snap *Snapshot) validatequick() {
	// validate epoch
	epochtm, now := time.Unix(0, snap.epoch), time.Now()
	if epochtm.After(now) {
		fmsg := "snapshot time %v comes after now: %v"
		panic(fmt.Errorf(fmsg, epochtm, now))
	}
	// validate footprint
	computed := (snap.n_zblocks * snap.zblocksize)
	computed += (snap.n_mblocks * snap.mblocksize)
	computed += (snap.n_vblocks * snap.vblocksize)
	computed += MarkerBlocksize + MarkerBlocksize /*infoblock*/
	ln := int64(len(snap.metadata))
	computed += (((ln - 1) / snap.mblocksize) + 1) * snap.mblocksize
	computed += MarkerBlocksize * int64(len(snap.readzs))
	computed += MarkerBlocksize * int64(len(snap.readvs))
	if computed != snap.footprint {
		fmsg := "computed footprint %v != actual %v"
		panic(fmt.Errorf(fmsg, computed, snap.footprint))
	}
	// validate footprint ratio to payload, only if payload is more that
	// seriouspayload (10 MB).
	seriouspayload := float64(1024 * 1024 * 10)
	if snap.keymem > 0 {
		payload := float64(snap.keymem) + float64(snap.n_count*zentrysize)
		payload += float64(snap.valmem) + float64(snap.n_count*mentrysize)
		ratio := payload / float64(snap.footprint)
		if payload > seriouspayload && ratio < 0.5 {
			panic(fmt.Errorf("payload/footprint %v exceeds %v", ratio, 0.5))
		}
	}
}

// Close snapshot, will release all in-memory resources but will keep
// the disk files. All Opened-Snapshots must be closed before it can
// be destoryed.
func (snap *Snapshot) Close() {
	if err := closereadat(snap.readm); err != nil {
		errorf("%v close %q: %v", snap.logprefix, snap.mfile, err)
	}
	for i, rd := range snap.readzs {
		err := closereadat(rd)
		if err != nil {
			errorf("%v close %q: %v", snap.logprefix, snap.zfiles[i], err)
		}
	}
	for i, rd := range snap.readvs {
		err := closereadat(rd)
		if err != nil {
			errorf("%v close: %q: %v", snap.logprefix, snap.vfiles[i], err)
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
	infof("%v purging disk snapshot", snap.logprefix)
	dirs := map[string]bool{}
	if snap.rw != nil {
		snap.rw.Lock()
		// lock and remove m-file and one or more z-files.
		if err := os.Remove(snap.mfile); err != nil {
			errorf("%v os.Remove(%q): %v", snap.logprefix, snap.mfile, err)
		}
		dirs[filepath.Dir(snap.mfile)] = true
		for _, zfile := range snap.zfiles {
			if err := os.Remove(zfile); err != nil {
				errorf("%v os.Remove(%q): %v", snap.logprefix, zfile, err)
			}
			dirs[filepath.Dir(zfile)] = true
		}
		for _, vfile := range snap.vfiles {
			if err := os.Remove(vfile); err != nil {
				errorf("%v os.Remove(%q): %v", snap.logprefix, vfile, err)
			}
			dirs[filepath.Dir(vfile)] = true
		}
		snap.rw.Unlock()
	}
	// remove lock file
	if err := os.Remove(snap.lockfile); err != nil {
		errorf("%v %v", snap.logprefix, err)
	}
	// remove directories path/name for each path in paths
	for dir := range dirs {
		if err := os.Remove(dir); err != nil {
			errorf("%v %v", snap.logprefix, err)
		}
	}
}

//---- Exported Read methods

// Get value for key, if value argument is not nil it will be used to
// copy the entry's value. Also returns entry's cas, whether entry is
// marked as deleted by LSM. If ok is false, then key is not found.
func (snap *Snapshot) Get(
	key, value []byte) (actualvalue []byte, cas uint64, deleted, ok bool) {

	var wkey []byte
	var lv lazyvalue
	var v []byte

	msize, zsize, vsize := snap.mblocksize, snap.zblocksize, snap.vblocksize
	buf := snap.rdpool.getreadbuffer(msize, zsize, vsize)

	shardidx, fpos := snap.findinmblock(key, buf)
	_, wkey, lv, cas, deleted, ok = snap.findinzblock(shardidx, fpos, key, buf)

	cmp := bytes.Compare(wkey, key)
	if cmp == 0 && value != nil {
		v, buf.vblock = lv.getactual(snap, buf.vblock)
		actualvalue = lib.Fixbuffer(value, int64(len(v)))
		copy(actualvalue, v)
	}

	snap.rdpool.putreadbuffer(buf)
	return actualvalue, cas, deleted, ok
}

func (snap *Snapshot) findinmblock(
	key []byte, buf *readbuffers) (shardidx byte, fpos int64) {

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
		m, mbindex = msnap(mblock), m.getindex(mbindex[:0])
		shardidx, fpos = m.findkey(0, mbindex, key)
	}
	return shardidx - 1, fpos
}

func (snap *Snapshot) findinzblock(
	shardidx byte, fpos int64,
	key []byte, buf *readbuffers) (
	index int, k []byte, lv lazyvalue, cas uint64, deleted, ok bool) {

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
	index, k, lv, cas, deleted, ok = z.findkey(0, zbindex, key)

	return
}

// BeginTxn is not allowed.
func (snap *Snapshot) BeginTxn(id uint64) api.Transactor {
	panic("not allowed")
}

// View start a read only transaction. Any number of views can be created
// on this snapshot provided they are not concurrently accessed.
func (snap *Snapshot) View(id uint64) api.Transactor {
	return snap.getview(id)
}

func (snap *Snapshot) abortview(view *View) error {
	snap.putview(view)
	return nil
}

// Scan return a full table iterator, if iteration is stopped before
// reaching end of table (io.EOF), application should call iterator
// with fin as true. EG: iter(true)
func (snap *Snapshot) Scan() api.Iterator {
	view := snap.getview(0xC0FFEE)
	cur, err := view.OpenCursor(nil)
	if err != nil {
		view.Abort()
		fmsg := "%v view(%v).OpenCursor(nil): %v"
		errorf(fmsg, snap.logprefix, view.id, err)
		return nil

	} else if cur == nil {
		view.Abort()
		fmsg := "%v view(%v).OpenCursor(nil) cursor is nil"
		errorf(fmsg, snap.logprefix, view.id)
		return nil
	}

	var key, value []byte
	var seqno uint64
	var deleted bool
	err = nil
	return func(fin bool) ([]byte, []byte, uint64, bool, error) {
		if err != nil {
			return nil, nil, 0, false, err

		} else if fin {
			_, _, _, _, err = cur.YNext(fin) // should return io.EOF
			view.Abort()
			return nil, nil, 0, false, err
		}
		key, value, seqno, deleted, err = cur.YNext(fin)
		if err != nil {
			view.Abort()
			return nil, nil, 0, false, err
		}
		return key, value, seqno, deleted, err
	}
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

func (snap *Snapshot) getview(id uint64) (view *View) {
	select {
	case view = <-snap.viewcache:
	default:
		view = &View{cursors: make([]*Cursor, 8)}
	}
	view.id, view.snap, view.cursors = id, snap, view.cursors[:0]
	return view
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
