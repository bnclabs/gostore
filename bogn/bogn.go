package bogr

import "sync"

import s "github.com/prataprc/gosettings"
import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/llrb"
import "github.com/prataprc/gostore/mvcc"

type Bogn struct {
	name string

	rw       sync.RWMutex
	snapshot unsafe.Pointer // *bogrsnapshot

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
	bogr := (&Bogn{name: name}).readsettings(setts)
	bogr.logprefix = fmt.Sprintf("BOGR [%v]", name)
	return bogr
}

func (bogr *Bogn) readsettings(setts s.Settings) *Bogn {
	bogr.memstore = setts.Bool("memstore")
	bogr.dgm = setts.Bool("dgm")
	bogr.workingset = setts.Bool("workingset")
	bogr.ratio = setts.Float64("ratio")
	bogr.flushperiod = time.Duration(setts.Int64("flushperiod")) * time.Second
	bogr.compacttick = time.Duration(setts.Int64("compacttick")) * time.Second
	bogr.compacttick = time.Duration(setts.Int64("compacttick")) * time.Second
	bogr.setts = setts
	return bogr
}

func (bogr *Bogn) currsnapshot() *bogrsnapshot {
	return (*bogrsnapshot)(atomic.LoadPointer(&bogr.snapshot))
}

func (bogr *Bogn) setheadsnapshot(snapshot *bogrsnapshot) {
	atomic.StorePointer(&bogr.snapshot, unsafe.Pointer(snapshot))
}

func (bogr *Bogn) newmemstore(memstore string) (*llrb.LLRB, error) {
	name := fmt.Sprintf("%v-mw", bogr.name)

	switch memstore {
	case "llrb":
		llrbsetts := bogr.setts.Section("llrb.").Trim("llrb.")
		index := llrb.NewLLRB(name, llrbsetts)
		return index, nil

	case "mvcc":
		llrbsetts := bogr.setts.Section("llrb.").Trim("llrb.")
		index := llrb.NewMVCC(name, llrbsetts)
		return index, nil
	}
	return fmt.Errorf("invalid memstore %q", memstore)
}

func (bogr *Bogn) mwmetadata() []byte {
	seqno := bogr.mw.Getseqno()
	metadata := map[string]interface{}{
		"seqno": seqno,
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		panic(err)
	}
	return data
}

//---- Exported Control methods

func (bogr *Bogn) ID() string {
	return bogr.name
}

func (bogr *Bogn) BeginTxn(id uint64) *Txn {
	return nil
}

func (bogr *Bogn) View(id uint64) *View {
}

func (bogr *Bogn) Clone(id uint64) *Bogn {
	return nil
}

func (bogr *Bogn) Stats() map[string]interface{} {
	return nil
}

func (bogr *Bogn) Log() {
	return
}

func (bogr *Bogn) Destroy() {
	return
}

//---- Exported read methods

func (bogr *Bogn) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	snapshot := bogr.currsnapshot()
	return snapshot.yget(key, value)
}

func (bogr *Bogn) Scan() api.Iterator {
	snapshot := bogr.currsnapshot()
	return snaps.reduceyscan()
}

//---- Exported write methods

func (bogr *Bogn) Delete(key, oldvalue []byte, lsm bool) ([]byte, uint64) {
	return bogr.currsnapshot().mw.Delete(key, oldvalue, lsm)
}

func (bogr *Bogn) Set(key, value, oldvalue []byte) (ov []byte, cas uint64) {
	return bogr.currsnapshot().mw.Set(key, value, oldvalue)
}

func (bogr *Bogn) SetCAS(
	key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error) {

	return bogr.currsnapshot().mw.SetCAS(key, value, oldvalue, cas)
}

//---- local methods
