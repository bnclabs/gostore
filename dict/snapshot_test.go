package dict

import "testing"
import "reflect"
import "bytes"
import "fmt"

import "github.com/prataprc/storage.go/api"

var _ = fmt.Sprintf("dummy")

func TestSnapshot(t *testing.T) {
	id := "testdict"
	d := NewDict(id)
	refkeys := [][]byte{
		[]byte("key1"), []byte("key2"), []byte("key3"), []byte("key4"),
	}
	refvals := [][]byte{
		[]byte("value1"), []byte("value2"), []byte("value3"), []byte("value4"),
	}
	seqno := uint64(0)
	for i := 0; i < len(refkeys); i++ {
		key, val := refkeys[i], refvals[i]
		seqno++
		d.Upsert(
			key, val,
			func(_ api.Index, _ int64, nd, _ api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				nd.SetBornseqno(seqno)
				return false
			})
	}
	refclock := api.Scalarclock(123)
	d.Setclock(refclock)

	snapch := make(chan api.IndexSnapshot, 1)
	if err := d.RSnapshot(snapch, true /*next*/); err != nil {
		t.Error(err)
	}
	snap := <-snapch

	keys := make([][]byte, 0)
	for _, key := range snap.(*DictSnapshot).Keys() {
		keys = append(keys, []byte(key))
	}
	if reflect.DeepEqual(keys, refkeys) == false {
		fmt.Println(refkeys)
		fmt.Println(keys)
		t.Errorf("keys() mismatch")
	}
	if snap.Count() != 4 {
		t.Errorf("expected %v, got %v", 4, snap.Count())
	}
	if snap.ID() != id+"-snap1" {
		t.Errorf("expected %v, got %v", id, snap.ID())
	}
	if clock := snap.Getclock(); clock != refclock {
		t.Errorf("expected %v, got %v", refclock, clock)
	}

	snap.Min(
		func(_ api.Index, _ int64, nd, _ api.Node, err error) bool {
			if bytes.Compare(nd.Key(), refkeys[0]) != 0 {
				t.Errorf("expected %q, got %q", refkeys[0], nd.Key())
			}
			if bytes.Compare(nd.Value(), refvals[0]) != 0 {
				t.Errorf("expected %q, got %q", refvals[0], nd.Value())
			}
			return true
		})
	snap.Max(
		func(_ api.Index, _ int64, nd, _ api.Node, err error) bool {
			if bytes.Compare(nd.Key(), refkeys[3]) != 0 {
				t.Errorf("expected %q, got %q", refkeys[3], nd.Key())
			}
			if bytes.Compare(nd.Value(), refvals[3]) != 0 {
				t.Errorf("expected %q, got %q", refvals[3], nd.Value())
			}
			return true
		})

	index := 0
	snap.Range(nil, nil, "both", false, /*reverse*/
		func(_ api.Index, _ int64, nd, _ api.Node, err error) bool {
			refkey, refval := refkeys[index], refvals[index]
			if bytes.Compare(nd.Key(), refkey) != 0 {
				t.Errorf("expected %q, got %q", refkey, nd.Key())
			}
			if bytes.Compare(nd.Value(), refval) != 0 {
				t.Errorf("expected %q, got %q", refval, nd.Value())
			}
			index++
			return true
		})
	if index != int(snap.Count()) {
		t.Errorf("not exhausted")
	}

	index = int(snap.Count()) - 1
	snap.Range(nil, nil, "both", true, /*reverse*/
		func(_ api.Index, _ int64, nd, _ api.Node, err error) bool {
			refkey, refval := refkeys[index], refvals[index]
			if bytes.Compare(nd.Key(), refkey) != 0 {
				t.Errorf("expected %q, got %q", refkey, nd.Key())
			}
			if bytes.Compare(nd.Value(), refval) != 0 {
				t.Errorf("expected %q, got %q", refval, nd.Value())
			}
			index--
			return true
		})
	if index != -1 {
		t.Errorf("not exhausted")
	}

	iter, index := snap.Iterate(nil, nil, "both", false /*reverse*/), 0
	for nd := iter.Next(); nd != nil; nd = iter.Next() {
		refkey, refval := refkeys[index], refvals[index]
		if bytes.Compare(nd.Key(), refkey) != 0 {
			t.Errorf("expected %q, got %q", refkey, nd.Key())
		}
		if bytes.Compare(nd.Value(), refval) != 0 {
			t.Errorf("expected %q, got %q", refval, nd.Value())
		}
		index++
	}
	if index != int(snap.Count()) {
		t.Errorf("not exhausted")
	}
	iter.Close()

	if err := d.Destroy(); err != nil {
		t.Error(err)
	}
	if snap.Isactive() == true {
		t.Errorf("expected false")
	}
}
