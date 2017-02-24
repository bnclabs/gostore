package llrb

import "fmt"

import "bytes"
import "math/rand"
import "testing"
import "reflect"

import "github.com/prataprc/storage.go/api"

var _ = fmt.Sprintf("dummy")

func TestLSMRange(t *testing.T) {
	setts := Defaultsettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.deadseqno"] = true
	setts["metadata.vbuuid"] = true
	setts["markdelete"] = true
	llrb1 := NewLLRB("first", setts)
	llrb2 := NewLLRB("second", setts)
	llrb3 := NewLLRB("third", setts)
	llrb4 := NewLLRB("fourth", setts)

	refsetts := setts.Section("")
	refsetts["markdelete"] = false
	refllrb := NewLLRB("reference", refsetts)

	indexes := []api.Index{llrb1, llrb2, llrb3, llrb4}
	entries, ops, n_testcases, seqno := 50, 1000, 100000, uint64(0)
	for i := 0; i < ops; i++ {
		seqno++
		key := []byte(fmt.Sprintf("key%05v", (i%entries)+1))
		value := []byte(fmt.Sprintf("value%05v", i+1))
		rnd := rand.Intn(100)
		index := indexes[rnd%len(indexes)]
		if rnd < 80 { // upsert
			//fmt.Printf("upsert %q %q %q\n", index.ID(), key, value)
			index.Upsert(
				key, value,
				func(_ api.Index, _ int64, nnd, ond api.Node, err error) bool {
					if err != nil {
						t.Error(err)
					}
					nnd.Setvbno(100).SetVbuuid(1000).SetBornseqno(seqno)
					return true
				})
			refllrb.Upsert(
				key, value,
				func(_ api.Index, _ int64, nnd, ond api.Node, err error) bool {
					if err != nil {
						t.Error(err)
					}
					nnd.Setvbno(100).SetVbuuid(1000).SetBornseqno(seqno)
					return true
				})

		} else {
			index := indexes[rand.Intn(len(indexes))]
			if rnd < 85 {
				index.DeleteMin(
					func(_ api.Index, _ int64, nnd, _ api.Node, err error) bool {
						if err != nil {
							t.Error(err)
						} else if nnd != nil {
							key = nnd.Key()
							nnd.Setvbno(100).SetVbuuid(1000).SetDeadseqno(seqno)
						}
						return true
					})
				//fmt.Printf("delmin %q %q %q\n", index.ID(), key, value)
			} else if rnd < 90 {
				index.DeleteMax(
					func(_ api.Index, _ int64, nnd, _ api.Node, err error) bool {
						if err != nil {
							t.Error(err)
						} else if nnd != nil {
							key = nnd.Key()
							nnd.Setvbno(100).SetVbuuid(1000).SetDeadseqno(seqno)
						}
						return true
					})
				//fmt.Printf("delmax %q %q %q\n", index.ID(), key, value)
			} else {
				//fmt.Printf("delete %q %q %q\n", index.ID(), key, value)
				index.Delete(
					key,
					func(_ api.Index, _ int64, nnd, _ api.Node, err error) bool {
						if err != nil {
							t.Error(err)
						} else if nnd != nil {
							nnd.Setvbno(100).SetVbuuid(1000).SetDeadseqno(seqno)
						}
						return true
					})
			}
			refllrb.Delete(
				key,
				func(_ api.Index, _ int64, nnd, ond api.Node, err error) bool {
					if err != nil {
						t.Error(err)
					} else if nnd != nil {
						nnd.Setvbno(100).SetVbuuid(1000).SetDeadseqno(seqno)
					}
					return true
				})
		}
	}

	for i := ops; i < ops+10; i++ {
		seqno++
		key := []byte(fmt.Sprintf("key%05v", i))
		rnd := rand.Intn(100)
		index := indexes[rnd%len(indexes)]
		index.Delete(
			key,
			func(_ api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				if nnd != nil {
					nnd.Setvbno(100).SetVbuuid(1000).SetDeadseqno(seqno)
				}
				return true
			})
		refllrb.Delete(key, nil)
	}

	keys := make([][]byte, 0, entries)
	for i := 1; i <= entries; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key%05v", i)))
	}
	keys = append(keys, []byte(nil))
	inclusions := []string{"none", "low", "high", "both"}

	testcases := make([][]interface{}, 0)
	for i := 0; i < n_testcases; i++ {
		testcases = append(testcases, []interface{}{
			keys[rand.Intn(len(keys))],
			keys[rand.Intn(len(keys))],
			inclusions[rand.Intn(len(inclusions))],
		})
	}
	//testcases = append(testcases, []interface{}{
	//	[]byte("key00013"),
	//	[]byte("key00019"),
	//	"both",
	//})

	for _, testcase := range testcases {
		low, high := testcase[0].([]byte), testcase[1].([]byte)
		incl := testcase[2].(string)
		//t.Logf("%q %q %q", testcase[0], testcase[1], testcase[2])

		// ascending
		iters := []api.IndexIterator{}
		for _, index := range indexes {
			iter := index.Iterate(low, high, incl, false)
			iters = append(iters, iter)
		}
		iter := api.LSMRange(false /*reverse*/, iters...)
		refiter := refllrb.Iterate(low, high, incl, false)
		verifylsm(t, iter, refiter, false /*merge*/)
		if iter != nil {
			iter.Close()
		}
		if refiter != nil {
			refiter.Close()
		}

		// descending
		iters = []api.IndexIterator{}
		for _, index := range indexes {
			iter := index.Iterate(low, high, incl, true)
			iters = append(iters, iter)
		}
		iter = api.LSMRange(true /*reverse*/, iters...)
		refiter = refllrb.Iterate(low, high, incl, true)
		verifylsm(t, iter, refiter, false /*merge*/)
		if iter != nil {
			iter.Close()
		}
		if refiter != nil {
			refiter.Close()
		}
	}

	llrb1.Destroy()
	llrb2.Destroy()
	llrb3.Destroy()
	llrb4.Destroy()
	refllrb.Destroy()
}

func verifylsm(t *testing.T, iter, refiter api.IndexIterator, merge bool) {
	if iter == nil && refiter != nil {
		t.Errorf("iter is nil but refiter is not nil")
		return
	} else if iter != nil && refiter == nil {
		t.Errorf("iter is not nil but refiter is nil")
		return
	} else if iter == nil && refiter == nil {
		return
	}
	//for nd := iter.Next(); nd != nil; nd = iter.Next() {
	//	bseqno, dseqno, deleted := nd.Bornseqno(), nd.Deadseqno(), nd.IsDeleted()
	//	fmsg := "lsm %q-%q-%v-%v-%v\n"
	//	fmt.Printf(fmsg, nd.Key(), nd.Value(), bseqno, dseqno, deleted)
	//}
	//for refnd := refiter.Next(); refnd != nil; refnd = refiter.Next() {
	//	bseqno, dseqno := refnd.Bornseqno(), refnd.Deadseqno()
	//	deleted := refnd.IsDeleted()
	//	fmsg := "ref %q-%q-%v-%v-%v\n"
	//	fmt.Printf(fmsg, refnd.Key(), refnd.Value(), bseqno, dseqno, deleted)
	//}
	nd, refnd := iter.Next(), refiter.Next()
	for ; refnd != nil; nd, refnd = iter.Next(), refiter.Next() {
		//t.Logf("  nd %q %q\n", nd.Key(), nd.Value())
		//t.Logf("  rd %q %q\n", refnd.Key(), refnd.Value())
		if vb, refvb := nd.Vbno(), refnd.Vbno(); vb != refvb {
			t.Errorf("expected vbno %v, got %v", refvb, vb)
		}
		vbuuid, refvbuuid := nd.Vbuuid(), refnd.Vbuuid()
		if vbuuid != refvbuuid {
			t.Errorf("expected vbno %v, got %v", refvbuuid, vbuuid)
		}
		born, refborn := nd.Bornseqno(), refnd.Bornseqno()
		if born != refborn {
			t.Errorf("expected bornseqno %v, got %v", refborn, born)
		}
		dead, refdead := nd.Deadseqno(), refnd.Deadseqno()
		if dead != refdead {
			t.Errorf("expected dead %v, got %v", refdead, dead)
		}
		key, refkey := nd.Key(), refnd.Key()
		if bytes.Compare(key, refkey) != 0 {
			t.Errorf("expected key %q, got %q", refkey, key)
		}
		value, refvalue := nd.Value(), refnd.Value()
		if bytes.Compare(value, refvalue) != 0 {
			t.Logf("for key %q %q", refnd.Key(), nd.Key())
			t.Errorf("expected value %q, got %q", refvalue, value)
		}
	}
	for ; nd != nil; nd = iter.Next() {
		fmsg := "error more nd : %q %q %v\n"
		t.Errorf(fmsg, nd.Key(), nd.Value(), nd.IsDeleted())
	}
	ndnil := nd == nil || reflect.ValueOf(nd).IsNil()
	refndnil := refnd == nil || reflect.ValueOf(refnd).IsNil()
	if ndnil == true && refndnil == false {
		t.Errorf("nd is nil but refnd is not nil")
	} else if ndnil == false && refndnil == true {
		t.Errorf("nd is not nil but refnd is nil")
	}
}
