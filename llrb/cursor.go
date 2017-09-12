package llrb

import "unsafe"

type Cursor struct {
	txn   *Txn
	stack []uintptr
}

func (cur *Cursor) opencursor(key []byte) *Cursor {
	var root *Llrbnode1
	switch db := cur.txn.snapshot.(type) {
	case *LLRB1:
		root = (*Llrbnode1)(db.root)
	}
	cur.stack = cur.first(root, key, cur.stack)
	return cur
}

func (cur *Cursor) Txn() *Txn {
	return cur.txn
}

func (cur *Cursor) Key() (key []byte, deleted bool) {
	if len(cur.stack) == 0 {
		return nil, false
	}
	ptr := cur.stack[len(cur.stack)-1]
	if ptr == 0 {
		cur.stack = cur.next(cur.stack)
		ptr = cur.stack[len(cur.stack)-1]
	}
	nd := (*Llrbnode1)(unsafe.Pointer(ptr & (^uintptr(0x3))))
	return nd.getkey(), nd.isdeleted()
}

func (cur *Cursor) Value() []byte {
	if len(cur.stack) == 0 {
		return nil
	}
	ptr := cur.stack[len(cur.stack)-1]
	if ptr == 0 {
		cur.stack = cur.next(cur.stack)
		ptr = cur.stack[len(cur.stack)-1]
	}
	nd := (*Llrbnode1)(unsafe.Pointer(ptr & (^uintptr(0x3))))
	return nd.Value()
}

func (cur *Cursor) GetNext() (key, value []byte, deleted bool) {
	cur.stack = cur.next(cur.stack)
	key, deleted = cur.Key()
	value = cur.Value()
	return
}

func (cur *Cursor) Set(key, value, oldvalue []byte) []byte {
	return cur.txn.Set(key, value, oldvalue)
}

func (cur *Cursor) Delete(key, oldvalue []byte, lsm bool) []byte {
	return cur.Delete(key, oldvalue, lsm)
}

func (cur *Cursor) YNext() (key, value []byte, seqno uint64, deleted bool) {
	cur.stack = cur.next(cur.stack)
	ptr := cur.stack[len(cur.stack)-1]
	nd := (*Llrbnode1)(unsafe.Pointer(ptr & (^uintptr(0x3))))
	key, seqno, deleted = nd.getkey(), nd.getseqno(), nd.isdeleted()
	value = nd.Value()
	return
}

func (cur *Cursor) first(
	root *Llrbnode1, key []byte, stack []uintptr) []uintptr {

	for nd := root; nd != nil; {
		ptr := (uintptr)(unsafe.Pointer(nd))
		if (ptr & 0x7) > 0 { // TODO: can be removed after testing.
			panic("impossible situation")

		} else if nd.ltkey(key, true) {
			stack = append(stack, ptr|0x3)
			nd = nd.right
			continue
		}
		stack = append(stack, ptr|0x1)
		nd = nd.left
	}
	stack = cur.popout(stack)
	return append(stack, 0) // NULL terminated list B-)
}

func (cur *Cursor) next(stack []uintptr) []uintptr {
	ptr := stack[len(stack)-1]
	if ptr == 0 { // initial case
		return cur.next(stack[:len(stack)-1])
	}
	nd := (*Llrbnode1)(unsafe.Pointer(ptr & (^uintptr(0x3))))
	link := ptr & 0x3
	if link != 1 { // TODO: can be removed after testing.
		panic("impossible situation")
	}
	stack[len(stack)-1] = ptr | 0x3
	stack = cur.leftmost(nd.right, stack)
	return cur.popout(stack)
}

func (cur *Cursor) popout(stack []uintptr) []uintptr {
	for i := len(stack) - 1; i >= 0; i-- {
		if (cur.stack[i] & 0x3) == 0x3 {
			return stack[:i+1]
		}
	}
	return stack
}

func (cur *Cursor) leftmost(nd *Llrbnode1, stack []uintptr) []uintptr {
	if nd != nil {
		ptr := (uintptr)(unsafe.Pointer(nd)) | 0x1
		stack = append(stack, ptr)
		return cur.leftmost(nd.left, stack)
	}
	return stack
}
