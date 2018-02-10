package llrb

import "io"
import "fmt"
import "unsafe"

var _ = fmt.Sprintf("")

// Cursor object maintains an active pointer into the index. Use OpenCursor
// on Txn object to create a new cursor.
type Cursor struct {
	txn   *Txn
	ynext bool
	stack []uintptr
}

func (cur *Cursor) opencursor(txn *Txn, snapshot interface{}, key []byte) *Cursor {
	cur.txn = txn // will be nil if opened on a view.

	var root *Llrbnode
	switch snap := snapshot.(type) {
	case *LLRB:
		root = snap.getroot()
	case *mvccsnapshot:
		root = snap.getroot()
	}
	cur.stack, cur.ynext = cur.first(root, key, cur.stack), false
	return cur
}

// Key return current key under the cursor. Returned byte slice will
// be a reference to index-key, hence must not be used after
// transaction is commited or aborted.
func (cur *Cursor) Key() (key []byte, deleted bool) {
	if len(cur.stack) == 0 {
		return nil, false
	}
	ptr := cur.stack[len(cur.stack)-1]
	nd := (*Llrbnode)(unsafe.Pointer(ptr & (^uintptr(0x3))))
	return nd.getkey(), nd.isdeleted()
}

// Value return current value under the cursor. Returned byte slice will
// be a reference to value in index, hence must not be used after
// transaction is commited or aborted.
func (cur *Cursor) Value() []byte {
	if len(cur.stack) == 0 {
		return nil
	}
	ptr := cur.stack[len(cur.stack)-1]
	nd := (*Llrbnode)(unsafe.Pointer(ptr & (^uintptr(0x3))))
	return nd.Value()
}

// GetNext move cursor to next entry in snapshot and return its key and
// value. Returned byte slices will be a reference to index entry, hence
// must not be used after transaction is committed or aborted.
func (cur *Cursor) GetNext() (key, value []byte, deleted bool, err error) {
	//fmt.Println(cur.stack)
	if len(cur.stack) == 0 {
		return nil, nil, false, io.EOF
	}
	cur.stack = cur.next(cur.stack)
	if len(cur.stack) == 0 {
		return nil, nil, false, io.EOF
	}
	key, deleted = cur.Key()
	value = cur.Value()
	return
}

// Set is an alias to txn.Set call. The current position of the cursor
// does not affect the set operation.
func (cur *Cursor) Set(key, value, oldvalue []byte) []byte {
	if cur.txn == nil {
		panic("Set not allowed on view-cursor")
	}
	return cur.txn.Set(key, value, oldvalue)
}

// Delete is an alias to txn.Delete call. The current position of the
// cursor does not affect the delete operation.
func (cur *Cursor) Delete(key, oldvalue []byte, lsm bool) []byte {
	if cur.txn == nil {
		panic("Delete not allowed on view-cursor")
	}
	return cur.txn.Delete(key, oldvalue, lsm)
}

// Delcursor deletes the entry at the cursor.
func (cur *Cursor) Delcursor(lsm bool) {
	if cur.txn == nil {
		panic("Delcursor not allowed on view-cursor")
	}
	key, _ := cur.Key()
	cur.txn.Delete(key, nil, lsm)
}

// YNext implements Iterator api, to iterate over the index. Typically
// used for lsm-sort.
func (cur *Cursor) YNext(
	fin bool) (key, value []byte, seqno uint64, deleted bool, err error) {

	if len(cur.stack) == 0 {
		return nil, nil, 0, false, io.EOF
	}
	if cur.ynext == false {
		cur.ynext = true
		ptr := cur.stack[len(cur.stack)-1]
		nd := (*Llrbnode)(unsafe.Pointer(ptr & (^uintptr(0x3))))
		key, seqno, deleted = nd.getkey(), nd.getseqno(), nd.isdeleted()
		value = nd.Value()
		return
	}
	cur.stack = cur.next(cur.stack)
	if len(cur.stack) == 0 {
		return nil, nil, 0, false, io.EOF
	}
	ptr := cur.stack[len(cur.stack)-1]
	nd := (*Llrbnode)(unsafe.Pointer(ptr & (^uintptr(0x3))))
	key, seqno, deleted = nd.getkey(), nd.getseqno(), nd.isdeleted()
	value = nd.Value()
	return
}

func (cur *Cursor) first(
	root *Llrbnode, key []byte, stack []uintptr) []uintptr {

	for nd := root; nd != nil; {
		ptr := (uintptr)(unsafe.Pointer(nd))
		if nd != nil && nd.ltkey(key, true) {
			stack = append(stack, ptr|0x3)
			nd = nd.right
			continue
		}
		stack = append(stack, ptr|0x0)
		nd = nd.left
	}
	return cur.popout(stack)
}

func (cur *Cursor) next(stack []uintptr) []uintptr {
	ptr := stack[len(stack)-1]
	nd := (*Llrbnode)(unsafe.Pointer(ptr & (^uintptr(0x3))))
	stack[len(stack)-1] = ptr | 0x3
	stack = cur.leftmost(nd.right, stack)
	return cur.popout(stack)
}

func (cur *Cursor) popout(stack []uintptr) []uintptr {
	i := len(stack) - 1
	for ; i >= 0; i-- {
		if (stack[i] & 0x3) == 0x3 {
			//fmt.Printf("popout %d\n", stack[i])
			continue
		}
		break
	}
	if stack = stack[:i+1]; len(stack) > 0 {
		stack[len(stack)-1] = stack[len(stack)-1] | 0x1
	}
	return stack
}

func (cur *Cursor) leftmost(nd *Llrbnode, stack []uintptr) []uintptr {
	if nd != nil {
		ptr := (uintptr)(unsafe.Pointer(nd)) | 0x0
		stack = append(stack, ptr)
		//fmt.Printf("leftmost %d\n", ptr)
		return cur.leftmost(nd.left, stack)
	}
	return stack
}
