package llrb

import "unsafe"
import "time"
import "fmt"
import "sync/atomic"
import "runtime/debug"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/log"
import "github.com/prataprc/storage.go/lib"

// LLRBWriter defines writer routine in mvcc mode.
type LLRBWriter struct {
	llrb    *LLRB
	waiters []chan api.IndexSnapshot
	reqch   chan []interface{}
	finch   chan bool
}

// MVCCWriter spawns a new writer. Only one writer routine is allowed for each
// LLRB instance.
func (llrb *LLRB) MVCCWriter() *LLRBWriter {
	if llrb.mvcc.enabled == false {
		panic(fmt.Errorf("MVCCWriter(): mvcc not enabled"))
	} else if llrb.mvcc.writer != nil {
		panic(fmt.Errorf("MVCCWriter(): concurrent writers are not allowed"))
	}

	chansize := llrb.config.Int64("mvcc.writer.chanbuffer")
	llrb.mvcc.writer = &LLRBWriter{
		llrb:    llrb,
		waiters: make([]chan api.IndexSnapshot, 0, 128),
		reqch:   make(chan []interface{}, chansize),
		finch:   make(chan bool),
	}
	log.Infof("%v starting mvcc writer ...\n", llrb.logprefix)
	go llrb.mvcc.writer.run()

	tick := llrb.config.Int64("mvcc.snapshot.tick")
	log.Infof("%v starting snapshot ticker (%v) ...\n", llrb.logprefix, tick)
	go llrb.mvcc.writer.snapshotticker(tick, llrb.mvcc.writer.finch)

	return llrb.mvcc.writer
}

const (
	// op commands
	cmdLlrbWriterUpsert byte = iota + 1
	cmdLlrbWriterUpsertMany
	cmdLlrbWriterDeleteMin
	cmdLlrbWriterDeleteMax
	cmdLlrbWriterDelete
	cmdLlrbWriterMakeSnapshot
	cmdLlrbWriterGetSnapshot
	cmdLlrbWriterPurgeSnapshot
	cmdLlrbWriterDestroy
	// maintanence commands
	cmdLlrbWriterStats
	cmdLlrbWriterFullstats
	cmdLlrbWriterValidate
	cmdLlrbWriterLog
)

func (writer *LLRBWriter) wupsert(
	key, value []byte, callb api.UpsertCallback) error {

	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterUpsert, key, value, callb, respch}
	_, err := lib.FailsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return err
}

func (writer *LLRBWriter) wupsertmany(
	keys, values [][]byte, callb api.UpsertCallback) error {

	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterUpsertMany, keys, values, callb, respch}
	_, err := lib.FailsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return err
}

func (writer *LLRBWriter) wdeleteMin(callb api.DeleteCallback) error {
	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterDeleteMin, callb, respch}
	_, err := lib.FailsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return err
}

func (writer *LLRBWriter) wdeleteMax(callb api.DeleteCallback) error {
	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterDeleteMax, callb, respch}
	_, err := lib.FailsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return err
}

func (writer *LLRBWriter) wdelete(key []byte, callb api.DeleteCallback) error {
	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterDelete, key, callb, respch}
	_, err := lib.FailsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return err
}

func (writer *LLRBWriter) makeSnapshot(id string) error {
	cmd := []interface{}{cmdLlrbWriterMakeSnapshot, id}
	return lib.FailsafePost(writer.reqch, cmd, writer.finch)
}

func (writer *LLRBWriter) getSnapshot(snapch chan api.IndexSnapshot) error {
	cmd := []interface{}{cmdLlrbWriterGetSnapshot, snapch}
	return lib.FailsafePost(writer.reqch, cmd, writer.finch)
}

func (writer *LLRBWriter) purgeSnapshot() error {
	cmd := []interface{}{cmdLlrbWriterPurgeSnapshot}
	return lib.FailsafePost(writer.reqch, cmd, writer.finch)
}

func (writer *LLRBWriter) stats() (map[string]interface{}, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{cmdLlrbWriterStats, respch}
	resp, err := lib.FailsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return resp[0].(map[string]interface{}), lib.ResponseError(err, resp, 1)
}

func (writer *LLRBWriter) fullstats() (map[string]interface{}, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{cmdLlrbWriterFullstats, respch}
	resp, err := lib.FailsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return resp[0].(map[string]interface{}), lib.ResponseError(err, resp, 1)
}

func (writer *LLRBWriter) validate() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{cmdLlrbWriterValidate, respch}
	_, err := lib.FailsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return err
}

func (writer *LLRBWriter) log(involved int, humanize bool) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{cmdLlrbWriterLog, involved, humanize, respch}
	lib.FailsafeRequest(writer.reqch, respch, cmd, writer.finch)
}

func (writer *LLRBWriter) destroy() error {
	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterDestroy, respch}
	_, err := lib.FailsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return err
}

func (writer *LLRBWriter) run() {
	reclaim := make([]*Llrbnode, 0, 64)
	llrb := writer.llrb

	dodestroy := func(ch chan bool) {
		for llrb.mvcc.snapshot != nil {
			writer.purgesnapshot(llrb)
			time.Sleep(100 * time.Millisecond) // TODO: no magic
		}
		close(ch)
	}

	defer func() {
		if r := recover(); r != nil {
			log.Errorf("%v writer() crashed: %v\n", llrb.logprefix, r)
			log.Errorf("\n%s", lib.GetStacktrace(2, debug.Stack()))
			ch := make(chan bool)
			dodestroy(ch)
			<-ch
		}

		close(writer.finch)
		time.Sleep(100 * time.Millisecond) // TODO: no magic
		for _, waiter := range writer.waiters {
			close(waiter)
		}
		for msg := range writer.reqch {
			if msg[0].(byte) == cmdLlrbWriterGetSnapshot {
				close(msg[1].(chan *LLRBSnapshot))
			}
		}
		log.Infof("%v ... stopping mvcc writer\n", llrb.logprefix)
	}()

	reclaimNodes := func(opname string, reclaim []*Llrbnode) {
		if len(reclaim) > 0 {
			llrb.mvcc.h_reclaims[opname].Add(int64(len(reclaim)))
			llrb.mvcc.n_reclaims += int64(len(reclaim))
			if llrb.mvcc.n_activess == 0 {
				// no snapshots are refering to these nodes, free them.
				for _, nd := range reclaim {
					llrb.freenode(nd)
				}
			} else {
				// snapshots do _refer_ to these nodes. remember to free later.
				llrb.mvcc.reclaim = append(llrb.mvcc.reclaim, reclaim...)
			}
		}
	}

loop:
	for {
		reclaim = reclaim[:0]
		msg := <-writer.reqch
		switch msg[0].(byte) {
		case cmdLlrbWriterUpsert:
			key, value := msg[1].([]byte), msg[2].([]byte)
			callb := msg[3].(api.UpsertCallback)
			respch := msg[4].(chan []interface{})

			reclaim = writer.mvccupsert(key, value, callb, reclaim)
			reclaimNodes("upsert", reclaim)
			close(respch)

		case cmdLlrbWriterUpsertMany:
			keys, values := msg[1].([][]byte), msg[2].([][]byte)
			callb := msg[3].(api.UpsertCallback)
			respch := msg[4].(chan []interface{})

			reclaim = writer.mvccupsertmany(
				keys, values, callb, reclaim,
				func(reclaim []*Llrbnode) []*Llrbnode {
					reclaimNodes("upmany", reclaim)
					return reclaim[:0]
				})
			reclaimNodes("upmany", reclaim)
			close(respch)

		case cmdLlrbWriterDeleteMin:
			callb := msg[1].(api.DeleteCallback)
			respch := msg[2].(chan []interface{})

			reclaim = writer.mvccdelmin(callb, reclaim)
			reclaimNodes("delmin", reclaim)
			close(respch)

		case cmdLlrbWriterDeleteMax:
			callb := msg[1].(api.DeleteCallback)
			respch := msg[2].(chan []interface{})

			reclaim = writer.mvccdelmax(callb, reclaim)
			reclaimNodes("delmax", reclaim)
			close(respch)

		case cmdLlrbWriterDelete:
			key, callb := msg[1].([]byte), msg[2].(api.DeleteCallback)
			respch := msg[3].(chan []interface{})

			reclaim = writer.mvccdelete(key, callb, reclaim)
			reclaimNodes("delete", reclaim)
			close(respch)

		case cmdLlrbWriterMakeSnapshot:
			id, ln := msg[1].(string), len(writer.waiters)
			writer.purgesnapshot(llrb)
			if ln > 0 {
				snapshot := llrb.NewSnapshot(id)
				for _, waiter := range writer.waiters {
					snapshot.Refer()
					waiter <- snapshot
				}
				fmsg := "%v $%v snapshot ACCOUNTED to %v waiters\n"
				log.Debugf(fmsg, llrb.logprefix, id, ln)
				writer.waiters = writer.waiters[:0]
			}

		case cmdLlrbWriterGetSnapshot:
			waiter := msg[1].(chan api.IndexSnapshot)
			log.Debugf("%v adding waiter for next snapshot\n", llrb.logprefix)
			writer.waiters = append(writer.waiters, waiter)

		case cmdLlrbWriterPurgeSnapshot:
			writer.purgesnapshot(llrb)

		case cmdLlrbWriterDestroy:
			ch := make(chan bool)
			dodestroy(ch)
			<-ch
			if llrb.mvcc.snapshot != nil {
				panic("writer(): snaphot after destroy? call the programmer")
			}
			break loop

		case cmdLlrbWriterStats:
			respch := msg[1].(chan []interface{})
			stats, err := llrb.stats()
			respch <- []interface{}{stats, err}

		case cmdLlrbWriterFullstats:
			respch := msg[1].(chan []interface{})
			stats, err := llrb.fullstats()
			respch <- []interface{}{stats, err}

		case cmdLlrbWriterValidate:
			respch := msg[1].(chan []interface{})
			llrb.validate(llrb.root)
			respch <- []interface{}{}

		case cmdLlrbWriterLog:
			involved := msg[1].(int)
			humanize := msg[2].(bool)
			respch := msg[3].(chan []interface{})
			llrb.log(involved, humanize)
			respch <- []interface{}{}
		}
	}
}

func (writer *LLRBWriter) mvccupsert(
	key, value []byte, callb api.UpsertCallback,
	reclaim []*Llrbnode) []*Llrbnode {

	var root, newnd, oldnd *Llrbnode

	llrb := writer.llrb
	llrb.mvcc.h_versions.Add(llrb.mvcc.n_activess)

	atomic.AddInt64(&llrb.mvcc.ismut, 1)

	root, newnd, oldnd, reclaim = writer.upsert(llrb.root, 1, key, value, reclaim)
	root.metadata().setblack()
	llrb.root = root
	llrb.upsertcounts(key, value, oldnd)
	if callb != nil {
		callb(llrb, 0, llndornil(newnd), llndornil(oldnd))
	}
	newnd.metadata().cleardirty()

	atomic.AddInt64(&llrb.mvcc.ismut, -1)
	return reclaim
}

func (writer *LLRBWriter) mvccupsertmany(
	keys, values [][]byte, callb api.UpsertCallback,
	reclaim []*Llrbnode, rfn func([]*Llrbnode) []*Llrbnode) []*Llrbnode {

	var root, newnd, oldnd *Llrbnode

	llrb := writer.llrb
	llrb.mvcc.h_versions.Add(llrb.mvcc.n_activess)

	atomic.AddInt64(&llrb.mvcc.ismut, 1)

	for i, k := range keys {
		var v []byte
		if len(values) > 0 {
			v = values[i]
		}
		root, newnd, oldnd, reclaim = writer.upsert(llrb.root, 1, k, v, reclaim)
		root.metadata().setblack()
		llrb.root = root
		llrb.upsertcounts(k, v, oldnd)
		if callb != nil {
			callb(llrb, int64(i), llndornil(newnd), llndornil(oldnd))
		}
		newnd.metadata().cleardirty()
		reclaim = rfn(reclaim)
	}

	atomic.AddInt64(&llrb.mvcc.ismut, -1)
	return reclaim
}

// returns root, newnd, oldnd, reclaim
func (writer *LLRBWriter) upsert(
	nd *Llrbnode, depth int64,
	key, value []byte,
	reclaim []*Llrbnode) (*Llrbnode, *Llrbnode, *Llrbnode, []*Llrbnode) {

	var oldnd, newnd *Llrbnode

	llrb := writer.llrb

	if nd == nil {
		newnd := llrb.newnode(key, value)
		llrb.h_upsertdepth.Add(depth)
		return newnd, newnd, nil, reclaim
	}
	reclaim = append(reclaim, nd)
	ndmvcc := llrb.clone(nd)

	ndmvcc = writer.walkdownrot23(ndmvcc)

	if ndmvcc.gtkey(llrb.mdsize, key) {
		ndmvcc.left, newnd, oldnd, reclaim =
			writer.upsert(ndmvcc.left, depth+1, key, value, reclaim)
	} else if ndmvcc.ltkey(llrb.mdsize, key) {
		ndmvcc.right, newnd, oldnd, reclaim =
			writer.upsert(ndmvcc.right, depth+1, key, value, reclaim)
	} else {
		oldnd = nd
		if ndmvcc.metadata().ismvalue() {
			if nv := ndmvcc.nodevalue(); nv != nil { // free the value if pres.
				nv.pool.Free(unsafe.Pointer(nv))
				ndmvcc = ndmvcc.setnodevalue(nil)
			}
		}
		if ndmvcc.metadata().ismvalue() && value != nil { // add new value.
			ptr, mpool := llrb.valarena.Alloc(int64(nvaluesize + len(value)))
			nv := (*nodevalue)(ptr)
			nv.pool = mpool
			ndmvcc = ndmvcc.setnodevalue(nv.setvalue(value))
		}
		ndmvcc.metadata().setdirty()
		newnd = ndmvcc
		llrb.h_upsertdepth.Add(depth)
	}

	ndmvcc, reclaim = writer.walkuprot23(ndmvcc, reclaim)
	return ndmvcc, newnd, oldnd, reclaim
}

func (writer *LLRBWriter) mvccdelmin(
	callb api.DeleteCallback, reclaim []*Llrbnode) []*Llrbnode {

	var root, deleted *Llrbnode

	llrb := writer.llrb
	llrb.mvcc.h_versions.Add(llrb.mvcc.n_activess)

	atomic.AddInt64(&llrb.mvcc.ismut, 1)

	root, deleted, reclaim = writer.deletemin(llrb.root, reclaim)
	if root != nil {
		root.metadata().setblack()
	}
	llrb.root = root
	llrb.delcount(deleted)
	if callb != nil {
		callb(llrb, llndornil(deleted))
	}

	atomic.AddInt64(&llrb.mvcc.ismut, -1)
	return reclaim
}

// using 2-3 trees, returns root, deleted, reclaim
func (writer *LLRBWriter) deletemin(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, *Llrbnode, []*Llrbnode) {

	var deleted *Llrbnode

	if nd == nil {
		return nil, nil, reclaim
	}

	reclaim = append(reclaim, nd)
	ndmvcc := writer.llrb.clone(nd)

	if ndmvcc.left == nil {
		reclaim = append(reclaim, ndmvcc)
		return nil, ndmvcc, reclaim
	}

	if !isred(ndmvcc.left) && !isred(ndmvcc.left.left) {
		ndmvcc, reclaim = writer.moveredleft(ndmvcc, reclaim)
	}

	ndmvcc.left, deleted, reclaim = writer.deletemin(ndmvcc.left, reclaim)
	ndmvcc, reclaim = writer.fixup(ndmvcc, reclaim)

	return ndmvcc, deleted, reclaim
}

func (writer *LLRBWriter) mvccdelmax(
	callb api.DeleteCallback, reclaim []*Llrbnode) []*Llrbnode {

	var root, deleted *Llrbnode

	llrb := writer.llrb
	llrb.mvcc.h_versions.Add(llrb.mvcc.n_activess)

	atomic.AddInt64(&llrb.mvcc.ismut, 1)

	root, deleted, reclaim = writer.deletemax(llrb.root, reclaim)
	if root != nil {
		root.metadata().setblack()
	}
	llrb.root = root
	llrb.delcount(deleted)
	if callb != nil {
		callb(llrb, llndornil(deleted))
	}

	atomic.AddInt64(&llrb.mvcc.ismut, -1)
	return reclaim
}

// using 2-3 trees, returns root, deleted, reclaim
func (writer *LLRBWriter) deletemax(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, *Llrbnode, []*Llrbnode) {

	var deleted *Llrbnode

	if nd == nil {
		return nil, nil, reclaim
	}
	reclaim = append(reclaim, nd)
	ndmvcc := writer.llrb.clone(nd)

	if isred(ndmvcc.left) {
		ndmvcc, reclaim = writer.rotateright(ndmvcc, reclaim)
	}
	if ndmvcc.right == nil {
		reclaim = append(reclaim, ndmvcc)
		return nil, ndmvcc, reclaim
	}

	if !isred(ndmvcc.right) && !isred(ndmvcc.right.left) {
		ndmvcc, reclaim = writer.moveredright(ndmvcc, reclaim)
	}

	ndmvcc.right, deleted, reclaim = writer.deletemax(ndmvcc.right, reclaim)
	ndmvcc, reclaim = writer.fixup(ndmvcc, reclaim)

	return ndmvcc, deleted, reclaim
}

func (writer *LLRBWriter) mvccdelete(
	key []byte, callb api.DeleteCallback, reclaim []*Llrbnode) []*Llrbnode {

	var root, deleted *Llrbnode

	llrb := writer.llrb
	llrb.mvcc.h_versions.Add(llrb.mvcc.n_activess)

	atomic.AddInt64(&llrb.mvcc.ismut, 1)
	atomic.AddInt64(&llrb.mvcc.ismut, -1)

	root, deleted, reclaim = writer.delete(llrb.root, key, reclaim)
	if root != nil {
		root.metadata().setblack()
	}
	llrb.root = root
	llrb.delcount(deleted)
	if callb != nil {
		callb(llrb, llndornil(deleted))
	}
	return reclaim
}

// using 2-3 trees, returns root, deleted, reclaim
func (writer *LLRBWriter) delete(
	nd *Llrbnode, key []byte,
	reclaim []*Llrbnode) (*Llrbnode, *Llrbnode, []*Llrbnode) {

	var newnd, deleted *Llrbnode

	llrb := writer.llrb
	if nd == nil {
		return nil, nil, reclaim
	}
	reclaim = append(reclaim, nd)
	ndmvcc := llrb.clone(nd)

	if ndmvcc.gtkey(llrb.mdsize, key) {
		if ndmvcc.left == nil { // key not present. Nothing to delete
			return ndmvcc, nil, reclaim
		}
		if !isred(ndmvcc.left) && !isred(ndmvcc.left.left) {
			ndmvcc, reclaim = writer.moveredleft(ndmvcc, reclaim)
		}
		ndmvcc.left, deleted, reclaim = writer.delete(ndmvcc.left, key, reclaim)

	} else {
		if isred(ndmvcc.left) {
			ndmvcc, reclaim = writer.rotateright(ndmvcc, reclaim)
		}

		// If @key equals @h.Item and no right children at @h
		if !ndmvcc.ltkey(llrb.mdsize, key) && ndmvcc.right == nil {
			reclaim = append(reclaim, ndmvcc)
			return nil, ndmvcc, reclaim
		}

		if ndmvcc.right != nil &&
			!isred(ndmvcc.right) && !isred(ndmvcc.right.left) {

			ndmvcc, reclaim = writer.moveredright(ndmvcc, reclaim)
		}

		// If @key equals @h.Item, and (from above) 'h.Right != nil'
		if !ndmvcc.ltkey(llrb.mdsize, key) {
			var subd *Llrbnode
			ndmvcc.right, subd, reclaim = writer.deletemin(ndmvcc.right, reclaim)
			if subd == nil {
				panic("delete(): fatal logic, call the programmer")
			}
			newnd = llrb.clone(subd)
			newnd.left, newnd.right = ndmvcc.left, ndmvcc.right
			if ndmvcc.metadata().isdirty() {
				//newnd.metadata().setdirty()
				panic("delete(): unexpected dirty node, call the programmer")
			}
			if ndmvcc.metadata().isblack() {
				newnd.metadata().setblack()
			} else {
				newnd.metadata().setred()
			}
			if newnd.metadata().ismvalue() {
				newnd.nodevalue().setvalue(subd.nodevalue().value())
			}
			deleted, ndmvcc = ndmvcc, newnd
			reclaim = append(reclaim, deleted)
		} else { // Else, @key is bigger than @ndmvcc
			ndmvcc.right, deleted, reclaim =
				writer.delete(ndmvcc.right, key, reclaim)
		}
	}
	ndmvcc, reclaim = writer.fixup(ndmvcc, reclaim)
	return ndmvcc, deleted, reclaim
}

//---- MVCC tree operations.

// rotation driver routines for 2-3 algorithm - mvcc

func (writer *LLRBWriter) walkdownrot23(nd *Llrbnode) *Llrbnode {
	return nd
}

func (writer *LLRBWriter) walkuprot23(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, []*Llrbnode) {

	if isred(nd.right) && !isred(nd.left) {
		nd, reclaim = writer.rotateleft(nd, reclaim)
	}

	if isred(nd.left) && isred(nd.left.left) {
		nd, reclaim = writer.rotateright(nd, reclaim)
	}

	if isred(nd.left) && isred(nd.right) {
		reclaim = writer.flip(nd, reclaim)
	}

	return nd, reclaim
}

func (writer *LLRBWriter) rotateleft(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, []*Llrbnode) {

	y, ok := writer.cloneifdirty(nd.right)
	if ok {
		reclaim = append(reclaim, nd.right)
	}

	if y.metadata().isblack() {
		panic("rotateleft(): rotating a black link ? call the programmer")
	}
	nd.right = y.left
	y.left = nd
	if nd.metadata().isblack() {
		y.metadata().setblack()
	} else {
		y.metadata().setred()
	}
	nd.metadata().setred()
	return y, reclaim
}

func (writer *LLRBWriter) rotateright(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, []*Llrbnode) {

	x, ok := writer.cloneifdirty(nd.left)
	if ok {
		reclaim = append(reclaim, nd.left)
	}

	if x.metadata().isblack() {
		panic("rotateright(): rotating a black link ? call the programmer")
	}
	nd.left = x.right
	x.right = nd
	if nd.metadata().isblack() {
		x.metadata().setblack()
	} else {
		x.metadata().setred()
	}
	nd.metadata().setred()
	return x, reclaim
}

// REQUIRE: Left and Right children must be present
func (writer *LLRBWriter) flip(nd *Llrbnode, reclaim []*Llrbnode) []*Llrbnode {
	x, ok := writer.cloneifdirty(nd.left)
	if ok {
		reclaim = append(reclaim, nd.left)
	}
	y, ok := writer.cloneifdirty(nd.right)
	if ok {
		reclaim = append(reclaim, nd.right)
	}

	x.metadata().togglelink()
	y.metadata().togglelink()
	nd.metadata().togglelink()
	nd.left, nd.right = x, y
	return reclaim
}

// REQUIRE: Left and Right children must be present
func (writer *LLRBWriter) moveredleft(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, []*Llrbnode) {

	reclaim = writer.flip(nd, reclaim)
	if isred(nd.right.left) {
		nd.right, reclaim = writer.rotateright(nd.right, reclaim)
		nd, reclaim = writer.rotateleft(nd, reclaim)
		reclaim = writer.flip(nd, reclaim)
	}
	return nd, reclaim
}

// REQUIRE: Left and Right children must be present
func (writer *LLRBWriter) moveredright(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, []*Llrbnode) {

	reclaim = writer.flip(nd, reclaim)
	if isred(nd.left.left) {
		nd, reclaim = writer.rotateright(nd, reclaim)
		reclaim = writer.flip(nd, reclaim)
	}
	return nd, reclaim
}

// REQUIRE: Left and Right children must be present
func (writer *LLRBWriter) fixup(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, []*Llrbnode) {

	if isred(nd.right) {
		nd, reclaim = writer.rotateleft(nd, reclaim)
	}
	if isred(nd.left) && isred(nd.left.left) {
		nd, reclaim = writer.rotateright(nd, reclaim)
	}
	if isred(nd.left) && isred(nd.right) {
		reclaim = writer.flip(nd, reclaim)
	}
	return nd, reclaim
}

func (writer *LLRBWriter) purgesnapshot(llrb *LLRB) {
	snapshot := llrb.mvcc.snapshot

loop:
	for snapshot != nil {
		refcount := atomic.LoadInt64(&snapshot.refcount)
		if refcount < 0 {
			panic("purgesnapshot(): snapshot refcount gone negative")
		} else if refcount > 0 {
			break loop
		}
		llrb.mvcc.h_bulkfree.Add(int64(len(snapshot.reclaim)))
		for _, nd := range snapshot.reclaim {
			snapshot.llrb.freenode(nd)
		}
		llrb.mvcc.n_activess--
		atomic.AddInt64(&llrb.mvcc.n_purgedss, 1)
		log.Debugf("%v snapshot PURGED\n", snapshot.logprefix)
		atomic.AddInt64(&llrb.n_lookups, snapshot.n_lookups)
		atomic.AddInt64(&llrb.n_ranges, snapshot.n_ranges)
		atomic.AddInt64(&llrb.mvcc.n_cclookups, snapshot.n_cclookups)
		atomic.AddInt64(&llrb.mvcc.n_ccranges, snapshot.n_ccranges)
		snapshot = snapshot.next
	}
	llrb.mvcc.snapshot = snapshot
}

func (writer *LLRBWriter) cloneifdirty(nd *Llrbnode) (*Llrbnode, bool) {
	if nd.metadata().isdirty() { // already cloned
		return nd, false
	}
	return writer.llrb.clone(nd), true
}
