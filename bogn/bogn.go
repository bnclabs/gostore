package bogn

import "sync"
import "time"

import s "github.com/prataprc/gosettings"
import "github.com/prataprc/gostore/api"

type Bogn struct {
	name     string
	snapshot unsafe.Pointer // *snapshot
	purgech  chan api.Index
	finch    chan struct{}

	memstore    string
	dgm         bool
	workingset  bool
	ratio       float64
	flushperiod time.Duration
	compacttick time.Duration
	setts       s.Settings
	logprefix   string
}

func NewBogn(name string, setts s.Settings) *Bogn {
	bogn := (&Bogn{name: name}).readsettings(setts)
	bogn.purgech = make(chan api.Index, 32)
	bogn.finch = make(chan struct{})
	bogn.logprefix = fmt.Sprintf("BOGN [%v]", name)

	go purger(bogn, bogn.purgech)
	go compactor(bogn, bogn.compacttick)

	return bogn
}

func (bogn *Bogn) readsettings(setts s.Settings) *Bogn {
	bogn.memstore = setts.Bool("memstore")
	bogn.dgm = setts.Bool("dgm")
	bogn.workingset = setts.Bool("workingset")
	bogn.ratio = setts.Float64("ratio")
	bogn.flushperiod = time.Duration(setts.Int64("flushperiod")) * time.Second
	bogn.compacttick = time.Duration(setts.Int64("compacttick")) * time.Second
	bogn.setts = setts
	return bogn
}

func (bogn *Bogn) currsnapshot() *snapshot {
	return (*snapshot)(atomic.LoadPointer(&bogn.snapshot))
}

func (bogn *Bogn) setheadsnapshot(snapshot *snapshot) {
	atomic.StorePointer(&bogn.snapshot, unsafe.Pointer(snapshot))
}

func (bogn *Bogn) newmemstore(
	memstore, level string, old api.Index) (api.Index, error) {

	name := fmt.Sprintf("%v-%v", bogn.name, level)

	switch memstore {
	case "llrb":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		index := llrb.NewLLRB(name, llrbsetts)
		if old != nil {
			index.Setseqno(old.(*llrb.LLRB).Getseqno())
		} else {
			index.Setseqno(0)
		}
		return index, nil

	case "mvcc":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		index := llrb.NewMVCC(name, llrbsetts)
		if old != nil {
			index.Setseqno(old.(*llrb.LLRB).Getseqno())
		} else {
			index.Setseqno(0)
		}
		return index, nil
	}
	return fmt.Errorf("invalid memstore %q", memstore)
}

func (bogn *Bogn) newdiskstore(level int) (*bubt.Bubt, error) {
	name := fmt.Spritnf("%v-level-%v", bogn.name, level)

	bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
	bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
	paths := bubtsetts.Strings("diskpaths")
	msize := bubtsetts.Int64("msize")
	zsize := bubtsetts.Int64("zsize")
	return bubt.NewBubt(name, paths, msize, zsize)
}

func (bogn *Bogn) mwmetadata() []byte {
	seqno := bogn.currsnapshot().getseqno()
	metadata := map[string]interface{}{
		"seqno": seqno,
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		panic(err)
	}
	return data
}

func (bogn *Bogn) isclosed() bool {
	select {
	case <-bogn.finch:
		return true
	default:
	}
	return false
}

//---- Exported Control methods

func (bogn *Bogn) ID() string {
	return bogn.name
}

func (bogn *Bogn) BeginTxn(id uint64) *Txn {
	return nil
}

func (bogn *Bogn) View(id uint64) *View {
}

func (bogn *Bogn) Clone(id uint64) *Bogn {
	return nil
}

func (bogn *Bogn) Stats() map[string]interface{} {
	return nil
}

func (bogn *Bogn) Log() {
	return
}

func (bogn *Bogn) Destroy() {
	for len(bogn.purgech) > 0 {
		time.Sleep(100 * time.Millisecond)
	}
	return
}

//---- Exported read methods

func (bogn *Bogn) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	return bogn.currsnapshot().yget(key, value)
}

func (bogn *Bogn) Scan() api.Iterator {
	return bogn.currsnapshot().iterator()
}

//---- Exported write methods

func (bogn *Bogn) Set(key, value, oldvalue []byte) (ov []byte, cas uint64) {
	bogn.rw.Lock()
	ov, cas = bogn.currsnapshot().set(key, value, oldvalue)
	bogn.rw.Unlock()
	return ov, cas
}

func (bogn *Bogn) SetCAS(
	key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error) {

	bogn.rw.Lock()
	ov, cas, err := bogn.currsnapshot().setCAS(key, value, oldvalue, cas)
	bogn.rw.Unlock()
	return ov, cas, err
}

func (bogn *Bogn) Delete(key, oldvalue []byte, lsm bool) ([]byte, uint64) {
	bogn.rw.Lock()
	ov, cas := bogn.currsnapshot().delete(key, oldvalue, lsm)
	bogn.rw.Unlock()
	return ov, cas
}

//---- local methods
