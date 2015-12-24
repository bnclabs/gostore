package storage

import "sync/atomic"
import "unsafe"

type LLRBWriter struct {
	llrb  *LLRB
	reqch chan []interface{}
	finch chan bool
}

func (llrb *LLRB) MVCCWriter() *LLRBWriter {
	if llrb.mvcc.enabled == false {
		panic("cannot create MVCCWriter(), mvcc not enabled")
	} else if llrb.mvcc.writer != nil {
		panic("concurrent writers are not allowed on llrb")
	}

	chansize := llrb.config["mvcc.writer.chanbuffer"].(int)
	llrb.mvcc.writer = &LLRBWriter{
		llrb:  llrb,
		reqch: make(chan []interface{}, chansize),
		finch: make(chan bool),
	}
	go llrb.mvcc.writer.run()
	return llrb.mvcc.writer
}

const (
	// op commands
	cmdLlrbWriterUpsert byte = iota + 1
	cmdLlrbWriterDeleteMin
	cmdLlrbWriterDeleteMax
	cmdLlrbWriterDelete
	cmdLlrbWriterDestroy
	// maintanence commands
	cmdLlrbWriterStatsMem
	cmdLlrbWriterStatsUpsert
	cmdLlrbWriterStatsHeight
	cmdLlrbWriterLogNodeutilz
	cmdLlrbWriterLogNodememory
	cmdLlrbWriterLogValueutilz
	cmdLlrbWriterLogValuememory
	cmdLlrbWriterLogUpsertdepth
	cmdLlrbWriterLogTreeheight

	cmdLlrbWriterClose
)

func (writer *LLRBWriter) Upsert(key, value []byte, callb LLRBUpsertCallback) {
	if key == nil {
		panic("upserting nil key")
	}
	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterUpsert, key, value, callb, respch}
	failsafeRequest(writer.reqch, respch, cmd, writer.finch)
}

func (writer *LLRBWriter) DeleteMin(callb LLRBDeleteCallback) {
	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterDeleteMin, callb, respch}
	failsafeRequest(writer.reqch, respch, cmd, writer.finch)
}

func (writer *LLRBWriter) DeleteMax(callb LLRBDeleteCallback) {
	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterDeleteMax, callb, respch}
	failsafeRequest(writer.reqch, respch, cmd, writer.finch)
}

func (writer *LLRBWriter) Delete(key []byte, callb LLRBDeleteCallback) {
	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterDelete, key, callb, respch}
	failsafeRequest(writer.reqch, respch, cmd, writer.finch)
}

func (writer *LLRBWriter) Destroy() bool {
	respch := make(chan []interface{}, 0)
	cmd := []interface{}{cmdLlrbWriterDestroy, respch}
	resp, _ := failsafeRequest(writer.reqch, respch, cmd, writer.finch)
	// responseError(err, resp, 1)
	return resp[0].(bool)
}

func (writer *LLRBWriter) StatsMem() map[string]interface{} {
	return nil
}

func (writer *LLRBWriter) StatsUpsert() map[string]interface{} {
	return nil
}

func (writer *LLRBWriter) StatsHeight() map[string]interface{} {
	return nil
}

func (writer *LLRBWriter) LogNodeutilz() {
}

func (writer *LLRBWriter) LogValueutilz() {
}

func (writer *LLRBWriter) LogNodememory() {
}

func (writer *LLRBWriter) LogValuememory() {
}

func (writer *LLRBWriter) LogUpsertdepth() {
}

func (writer *LLRBWriter) LogTreeheight() {
}

func (writer *LLRBWriter) run() {
	var root, newnd, oldnd *Llrbnode
	reclaim := make([]*Llrbnode, 0, 64)
	llrb := writer.llrb

	reclaimNodes := func(opname string, reclaim []*Llrbnode) {
		llrb.mvcc.cowednodes = append(llrb.mvcc.cowednodes, reclaim...)
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
			reclaimNodes("upsert", reclaim)
			root.metadata().setblack()
			atomic.StorePointer(&llrb.root, unsafe.Pointer(root))

			llrb.upsertcount(key, val, oldnd)

			if callb != nil {
				callb(newnd, oldnd)
			}

			llrb.freenode(oldnd)
			close(respch)

		case cmdLlrbWriterDeleteMin:
			callb := msg[1].(LLRBDeleteCallback)
			respch := msg[2].(chan []interface{})

			nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
			root, deleted, reclaim := writer.deletemin(nd, reclaim)
			reclaimNodes("delmin", reclaim)
			if root != nil {
				root.metadata().setblack()
			}
			atomic.StorePointer(&llrb.root, unsafe.Pointer(root))

			llrb.delcount(deleted)
			if callb != nil {
				callb(deleted)
			}
			llrb.freenode(deleted)
			close(respch)

		case cmdLlrbWriterDeleteMax:
			callb := msg[1].(LLRBDeleteCallback)
			respch := msg[2].(chan []interface{})

			nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
			root, deleted, reclaim := writer.deletemax(nd, reclaim)
			reclaimNodes("delmax", reclaim)
			if root != nil {
				root.metadata().setblack()
			}
			atomic.StorePointer(&llrb.root, unsafe.Pointer(root))

			llrb.delcount(deleted)
			if callb != nil {
				callb(deleted)
			}
			llrb.freenode(deleted)
			close(respch)

		case cmdLlrbWriterDelete:
			key, callb := msg[1].([]byte), msg[2].(LLRBDeleteCallback)
			respch := msg[3].(chan []interface{})

			nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
			root, deleted, reclaim := writer.delete(nd, key, reclaim)
			reclaimNodes("delete", reclaim)
			if root != nil {
				root.metadata().setblack()
			}
			atomic.StorePointer(&llrb.root, unsafe.Pointer(root))

			llrb.delcount(deleted)
			if callb != nil {
				callb(deleted)
			}
			llrb.freenode(deleted)
			close(respch)

		case cmdLlrbWriterDestroy:
		case cmdLlrbWriterStatsMem:
		case cmdLlrbWriterStatsUpsert:
		case cmdLlrbWriterStatsHeight:
		case cmdLlrbWriterLogNodeutilz:
		case cmdLlrbWriterLogNodememory:
		case cmdLlrbWriterLogValueutilz:
		case cmdLlrbWriterLogValuememory:
		case cmdLlrbWriterLogUpsertdepth:
		case cmdLlrbWriterLogTreeheight:
		case cmdLlrbWriterClose:
			break loop
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
		}
		if value != nil { // and new value if need be
			ptr, mpool := llrb.valarena.alloc(int64(nvaluesize + len(value)))
			nv := (*nodevalue)(ptr)
			nv.pool = mpool
			ndmvcc = ndmvcc.setnodevalue(nv.setvalue(value))
		}
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
	if nd.left == nil {
		reclaim = append(reclaim, nd)
		return nil, nd, reclaim
	}

	reclaim = append(reclaim, nd)
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
				panic("logic")
			}
			newnd = writer.llrb.clone(subd)
			newnd.left, newnd.right = ndmvcc.left, ndmvcc.right
			if ndmvcc.metadata().isdirty() {
				newnd.metadata().setdirty()
			}
			if ndmvcc.metadata().isblack() {
				newnd.metadata().setblack()
			} else {
				newnd.metadata().setred()
			}
			newnd.nodevalue().setvalue(subd.nodevalue().value())
			deleted, ndmvcc = ndmvcc, newnd
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

	reclaim = append(reclaim, nd.right)
	y := writer.llrb.clone(nd.right)
	if y.metadata().isblack() {
		panic("rotating a black link")
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

	reclaim = append(reclaim, nd.left)
	x := writer.llrb.clone(nd.left)
	if x.metadata().isblack() {
		panic("rotating a black link")
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
	reclaim = append(reclaim, nd.left, nd.right)
	x, y := writer.llrb.clone(nd.left), writer.llrb.clone(nd.right)
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
