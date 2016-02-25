package storage

import "sync/atomic"
import "unsafe"
import "time"
import "io"
import "errors"

type LLRBWriter struct {
	llrb    *LLRB
	waiters []chan *LLRBSnapshot
	reqch   chan []interface{}
	finch   chan bool
}

func (llrb *LLRB) MVCCWriter() *LLRBWriter {
	if llrb.mvcc.enabled == false {
		panic("cannot create MVCCWriter(), mvcc not enabled")
	} else if llrb.mvcc.writer != nil {
		panic("concurrent writers are not allowed on llrb")
	}

	chansize := llrb.config["mvcc.writer.chanbuffer"].(int)
	llrb.mvcc.writer = &LLRBWriter{
		llrb:    llrb,
		waiters: make([]chan *LLRBSnapshot, 0, 128),
		reqch:   make(chan []interface{}, chansize),
		finch:   make(chan bool),
	}
	log.Infof("%v starting mvcc writer ...\n", llrb.logPrefix)
	go llrb.mvcc.writer.run()

	tick := llrb.config["mvcc.snapshot.tick"].(int)
	log.Infof("%v starting snapshot ticker (%v) ...\n", llrb.logPrefix, tick)
	go llrb.mvcc.writer.snapshotticker(tick, llrb.mvcc.writer.finch)

	return llrb.mvcc.writer
}

const (
	// op commands
	cmdLlrbWriterUpsert byte = iota + 1
	cmdLlrbWriterDeleteMin
	cmdLlrbWriterDeleteMax
	cmdLlrbWriterDelete
	cmdLlrbWriterMakeSnapshot
	cmdLlrbWriterGetSnapshot
	cmdLlrbWriterDestroy
	// maintanence commands
	cmdLlrbWriterStatsMem
	cmdLlrbWriterStatsUpsert
	cmdLlrbWriterStatsHeight
	cmdLlrbWriterLogmemory
	cmdLlrbWriterLogUpsertdepth
	cmdLlrbWriterLogTreeheight
)

func (writer *LLRBWriter) Upsert(key, value []byte, callb LLRBUpsertCallback) error {
	if key == nil {
		return errors.New("upserting nil key")
	}
	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterUpsert, key, value, callb, respch}
	_, err := failsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return err
}

func (writer *LLRBWriter) DeleteMin(callb LLRBDeleteCallback) error {
	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterDeleteMin, callb, respch}
	_, err := failsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return err
}

func (writer *LLRBWriter) DeleteMax(callb LLRBDeleteCallback) error {
	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterDeleteMax, callb, respch}
	_, err := failsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return err
}

func (writer *LLRBWriter) Delete(key []byte, callb LLRBDeleteCallback) error {
	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterDelete, key, callb, respch}
	_, err := failsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return err
}

func (writer *LLRBWriter) MakeSnapshot(id string) error {
	cmd := []interface{}{cmdLlrbWriterMakeSnapshot, id}
	return failsafePost(writer.reqch, cmd, writer.finch)
}

func (writer *LLRBWriter) GetSnapshot(waiter chan *LLRBSnapshot) error {
	cmd := []interface{}{cmdLlrbWriterGetSnapshot, waiter}
	return failsafePost(writer.reqch, cmd, writer.finch)
}

func (writer *LLRBWriter) StatsMem() (map[string]interface{}, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{cmdLlrbWriterStatsMem, respch}
	resp, err := failsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return resp[0].(map[string]interface{}), err
}

func (writer *LLRBWriter) StatsUpsert() (map[string]interface{}, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{cmdLlrbWriterStatsUpsert, respch}
	resp, err := failsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return resp[0].(map[string]interface{}), err
}

func (writer *LLRBWriter) StatsHeight() (map[string]interface{}, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{cmdLlrbWriterStatsHeight, respch}
	resp, err := failsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return resp[0].(map[string]interface{}), err
}

func (writer *LLRBWriter) Logmemory(w io.Writer) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{cmdLlrbWriterLogmemory, w, respch}
	failsafeRequest(writer.reqch, respch, cmd, writer.finch)
}

func (writer *LLRBWriter) LogUpsertdepth() {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{cmdLlrbWriterLogUpsertdepth, respch}
	failsafeRequest(writer.reqch, respch, cmd, writer.finch)
}

func (writer *LLRBWriter) LogTreeheight() {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{cmdLlrbWriterLogTreeheight, respch}
	failsafeRequest(writer.reqch, respch, cmd, writer.finch)
}

func (writer *LLRBWriter) destroy() error {
	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterDestroy, respch}
	_, err := failsafeRequest(writer.reqch, respch, cmd, writer.finch)
	return err
}

func (writer *LLRBWriter) run() {
	var root, newnd, oldnd *Llrbnode
	reclaim := make([]*Llrbnode, 0, 64)
	llrb := writer.llrb

	defer func() {
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
		log.Infof("%v ... stopping mvcc writer\n", llrb.logPrefix)
	}()

	reclaimNodes := func(opname string, reclaim []*Llrbnode) {
		llrb.mvcc.reclaim = append(llrb.mvcc.reclaim, reclaim...)
		llrb.mvcc.reclaimstats[opname].add(int64(len(reclaim)))
	}

loop:
	for {
		reclaim = reclaim[:0]
		msg := <-writer.reqch
		switch msg[0].(byte) {
		case cmdLlrbWriterUpsert:
			key, val := msg[1].([]byte), msg[2].([]byte)
			callb := msg[3].(LLRBUpsertCallback)
			respch := msg[4].(chan []interface{})

			nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
			depth := int64(1) /*upsertdepth*/
			root, newnd, oldnd, reclaim =
				writer.upsert(nd, depth, key, val, reclaim)
			root.metadata().setblack()
			atomic.StorePointer(&llrb.root, unsafe.Pointer(root))

			llrb.upsertcounts(key, val, oldnd)

			if callb != nil {
				callb(llrb, newnd, oldnd)
			}
			if newnd.metadata().isdirty() == false {
				panic("expected this to be dirty")
			} else {
				newnd.metadata().cleardirty()
			}

			reclaimNodes("upsert", reclaim)
			close(respch)

		case cmdLlrbWriterDeleteMin:
			callb := msg[1].(LLRBDeleteCallback)
			respch := msg[2].(chan []interface{})

			nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
			root, deleted, reclaim := writer.deletemin(nd, reclaim)
			if root != nil {
				root.metadata().setblack()
			}
			atomic.StorePointer(&llrb.root, unsafe.Pointer(root))

			llrb.delcount(deleted)

			if callb != nil {
				callb(llrb, deleted)
			}

			reclaimNodes("delmin", reclaim)
			close(respch)

		case cmdLlrbWriterDeleteMax:
			callb := msg[1].(LLRBDeleteCallback)
			respch := msg[2].(chan []interface{})

			nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
			root, deleted, reclaim := writer.deletemax(nd, reclaim)
			if root != nil {
				root.metadata().setblack()
			}
			atomic.StorePointer(&llrb.root, unsafe.Pointer(root))

			llrb.delcount(deleted)

			if callb != nil {
				callb(llrb, deleted)
			}

			reclaimNodes("delmax", reclaim)
			close(respch)

		case cmdLlrbWriterDelete:
			key, callb := msg[1].([]byte), msg[2].(LLRBDeleteCallback)
			respch := msg[3].(chan []interface{})

			nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
			root, deleted, reclaim := writer.delete(nd, key, reclaim)
			if root != nil {
				root.metadata().setblack()
			}
			atomic.StorePointer(&llrb.root, unsafe.Pointer(root))

			llrb.delcount(deleted)

			if callb != nil {
				callb(llrb, deleted)
			}

			reclaimNodes("delete", reclaim)
			close(respch)

		case cmdLlrbWriterMakeSnapshot:
			id, ln := msg[1].(string), len(writer.waiters)
			snapshot := llrb.NewSnapshot(id)
			for _, waiter := range writer.waiters {
				waiter <- snapshot
				atomic.AddInt32(&snapshot.refcount, 1)
				close(waiter)
			}
			fmsg := "%v dispatched snapshot $%v to %v waiters\n"
			log.Debugf(fmsg, llrb.logPrefix, id, ln)
			writer.waiters = writer.waiters[:0]
			writer.purgesnapshot(llrb)

		case cmdLlrbWriterGetSnapshot:
			waiter := msg[1].(chan *LLRBSnapshot)
			log.Debugf("%v adding waiter for next snapshot\n", llrb.logPrefix)
			writer.waiters = append(writer.waiters, waiter)

		case cmdLlrbWriterDestroy:
			ch := make(chan bool)
			go func() {
				for writer.purgesnapshot(llrb) != "all" {
					time.Sleep(100 * time.Millisecond) // TODO: no magic
				}
				close(ch)
			}()
			<-ch
			break loop

		case cmdLlrbWriterStatsMem:
			respch := msg[1].(chan []interface{})
			stats := llrb.StatsMem()
			respch <- []interface{}{stats}

		case cmdLlrbWriterStatsUpsert:
			respch := msg[1].(chan []interface{})
			stats := llrb.StatsUpsert()
			respch <- []interface{}{stats}

		case cmdLlrbWriterStatsHeight:
			respch := msg[1].(chan []interface{})
			stats := llrb.StatsHeight()
			respch <- []interface{}{stats}

		case cmdLlrbWriterLogmemory:
			w := msg[1].(io.Writer)
			respch := msg[2].(chan []interface{})
			llrb.Logmemory(w)
			respch <- []interface{}{}

		case cmdLlrbWriterLogUpsertdepth:
			respch := msg[1].(chan []interface{})
			llrb.LogUpsertdepth()
			respch <- []interface{}{}

		case cmdLlrbWriterLogTreeheight:
			respch := msg[1].(chan []interface{})
			llrb.LogTreeheight()
			respch <- []interface{}{}
		}
	}
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
		llrb.upsertdepth.add(depth)
		return newnd, newnd, nil, reclaim
	}
	reclaim = append(reclaim, nd)
	ndmvcc := llrb.clone(nd)

	ndmvcc = writer.walkdownrot23(ndmvcc)

	if ndmvcc.gtkey(key) {
		ndmvcc.left, newnd, oldnd, reclaim =
			writer.upsert(ndmvcc.left, depth+1, key, value, reclaim)
	} else if ndmvcc.ltkey(key) {
		ndmvcc.right, newnd, oldnd, reclaim =
			writer.upsert(ndmvcc.right, depth+1, key, value, reclaim)
	} else {
		oldnd = nd
		if nv := ndmvcc.nodevalue(); nv != nil { // free the value if present
			nv.pool.free(unsafe.Pointer(nv))
			ndmvcc = ndmvcc.setnodevalue(nil)
		}
		if value != nil { // add new value if need be
			ptr, mpool := llrb.valarena.alloc(int64(nvaluesize + len(value)))
			nv := (*nodevalue)(ptr)
			nv.pool = mpool
			ndmvcc = ndmvcc.setnodevalue(nv.setvalue(value))
		}
		ndmvcc.metadata().setdirty()
		newnd = ndmvcc
		llrb.upsertdepth.add(depth)
	}

	ndmvcc, reclaim = writer.walkuprot23(ndmvcc, reclaim)
	return ndmvcc, newnd, oldnd, reclaim
}

// using 2-3 trees, returns root, deleted, reclaim
func (writer *LLRBWriter) deletemin(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, *Llrbnode, []*Llrbnode) {

	var deleted *Llrbnode

	if nd == nil {
		return nil, nil, reclaim
	}

	reclaim = append(reclaim, nd)

	if nd.left == nil {
		return nil, nd, reclaim
	}

	ndmvcc := writer.llrb.clone(nd)

	if !isred(ndmvcc.left) && !isred(ndmvcc.left.left) {
		ndmvcc, reclaim = writer.moveredleft(ndmvcc, reclaim)
	}

	ndmvcc.left, deleted, reclaim = writer.deletemin(ndmvcc.left, reclaim)
	ndmvcc, reclaim = writer.fixup(ndmvcc, reclaim)

	return ndmvcc, deleted, reclaim
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

// using 2-3 trees, returns root, deleted, reclaim
func (writer *LLRBWriter) delete(
	nd *Llrbnode, key []byte,
	reclaim []*Llrbnode) (*Llrbnode, *Llrbnode, []*Llrbnode) {

	var newnd, deleted *Llrbnode

	if nd == nil {
		return nil, nil, reclaim
	}
	reclaim = append(reclaim, nd)
	ndmvcc := writer.llrb.clone(nd)

	if ndmvcc.gtkey(key) {
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
		if !ndmvcc.ltkey(key) && ndmvcc.right == nil {
			reclaim = append(reclaim, ndmvcc)
			return nil, ndmvcc, reclaim
		}

		if ndmvcc.right != nil &&
			!isred(ndmvcc.right) && !isred(ndmvcc.right.left) {

			ndmvcc, reclaim = writer.moveredright(ndmvcc, reclaim)
		}

		// If @key equals @h.Item, and (from above) 'h.Right != nil'
		if !ndmvcc.ltkey(key) {
			var subd *Llrbnode
			ndmvcc.right, subd, reclaim = writer.deletemin(ndmvcc.right, reclaim)
			if subd == nil {
				panic("fatal logic, call the programmer")
			}
			newnd = writer.llrb.clone(subd)
			newnd.left, newnd.right = ndmvcc.left, ndmvcc.right
			if ndmvcc.metadata().isdirty() {
				//newnd.metadata().setdirty()
				panic("unexpected dirty node, call the programmer")
			}
			if ndmvcc.metadata().isblack() {
				newnd.metadata().setblack()
			} else {
				newnd.metadata().setred()
			}
			newnd.nodevalue().setvalue(subd.nodevalue().value())
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
		panic("rotating a black link ? call the programmer")
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
		panic("rotating a black link ? call the programmer")
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

func (writer *LLRBWriter) purgesnapshot(llrb *LLRB) string {
	location := &llrb.mvcc.snapshot
	upsnapshot := atomic.LoadPointer(location)
	count := 0
	for upsnapshot != nil {
		snapshot := (*LLRBSnapshot)(upsnapshot)
		if snapshot.ReclaimNodes() == false {
			break
		}
		upsnapshot = atomic.LoadPointer(&snapshot.next)
		fmsg := "%v purged snapshot $%v\n"
		log.Debugf(fmsg, writer.llrb.logPrefix, snapshot.Id())
		count++
	}
	atomic.StorePointer(location, upsnapshot)
	if upsnapshot == nil {
		return "all"
	}
	return "partial"
}

func (writer *LLRBWriter) cloneifdirty(nd *Llrbnode) (*Llrbnode, bool) {
	if nd.metadata().isdirty() { // already cloned
		return nd, false
	}
	return writer.llrb.clone(nd), true
}
