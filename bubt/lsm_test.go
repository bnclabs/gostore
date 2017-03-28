package bubt

import "fmt"
import "bytes"
import "time"
import "testing"
import "reflect"
import "os"
import "math/rand"
import "path/filepath"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/llrb"

var _ = fmt.Sprintf("dummy")

func TestLSMRange(t *testing.T) {
	setts := Defaultsettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.deadseqno"] = true
	setts["metadata.vbuuid"] = true
	setts["lsm"] = true
	llrb1 := llrb.NewLLRB("first", setts)
	llrb2 := llrb.NewLLRB("second", setts)
	llrb3 := llrb.NewLLRB("third", setts)
	llrb4 := llrb.NewLLRB("fourth", setts)

	refsetts := setts.Section("")
	refsetts["lsm"] = false
	refllrb := llrb.NewLLRB("reference", refsetts)

	indexes := []api.Index{llrb1, llrb2, llrb3, llrb4}
	entries, ops, n_testcases := 50, 1000, 100000
	buildlsmindexes(t, indexes, refllrb, ops, entries, false /*mvcc*/)

	// build keyset and inclusion set.
	keys := make([][]byte, 0, entries)
	for i := 1; i <= entries; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key%05v", i)))
	}
	keys = append(keys, []byte(nil))
	inclusions := []string{"none", "low", "high", "both"}

	// test cases with valid set of parameters
	validtests := func(bubtindexes []api.Index) {
		testcases := make([][]interface{}, 0)
		for i := 0; i < len(keys); i++ {
			for j := i; j < len(keys); j++ {
				for _, incl := range inclusions {
					testcases = append(testcases, []interface{}{
						keys[i], keys[j], incl,
					})
				}
			}
		}
		applyranges(t, bubtindexes, refllrb, false /*mvcc*/, testcases)
	}

	// test cases with random parameters
	randomtests := func(bubtindexes []api.Index) {
		testcases := make([][]interface{}, 0)
		for i := 0; i < n_testcases; i++ {
			testcases = append(testcases, []interface{}{
				keys[rand.Intn(len(keys))],
				keys[rand.Intn(len(keys))],
				inclusions[rand.Intn(len(inclusions))],
			})
		}
		applyranges(t, bubtindexes, refllrb, false /*mvcc*/, testcases)
	}

	//-- flush to bubt with separate datafile
	bubtindexes := []api.Index{}
	for _, index := range indexes {
		bubtindex := buildbubtindex(t, index, true /*datafile*/, false /*mvcc*/)
		bubtindexes = append(bubtindexes, bubtindex)
	}
	validtests(bubtindexes)
	randomtests(bubtindexes)
	destroyindexes(t, bubtindexes, nil)

	//-- flush to bubt without separate datafile
	bubtindexes = []api.Index{}
	for _, index := range indexes {
		bubtindex := buildbubtindex(t, index, false /*datafile*/, false /*mvcc*/)
		bubtindexes = append(bubtindexes, bubtindex)
	}
	validtests(bubtindexes)
	randomtests(bubtindexes)
	destroyindexes(t, bubtindexes, nil)

	//testcases = append(testcases, []interface{}{
	//	[]byte("key00013"),
	//	[]byte("key00019"),
	//	"both",
	//})

	destroyindexes(t, indexes, refllrb)
}

func TestLSMMerge(t *testing.T) {
	setts := Defaultsettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.deadseqno"] = true
	setts["metadata.vbuuid"] = true
	setts["lsm"] = true
	llrb1 := llrb.NewLLRB("first", setts)
	llrb2 := llrb.NewLLRB("second", setts)
	llrb3 := llrb.NewLLRB("third", setts)
	llrb4 := llrb.NewLLRB("fourth", setts)

	refsetts := setts.Section("")
	refsetts["lsm"] = false
	refllrb := llrb.NewLLRB("reference", refsetts)

	indexes := []api.Index{llrb1, llrb2, llrb3, llrb4}
	entries, ops, n_testcases := 50, 1000, 100000
	buildlsmindexes(t, indexes, refllrb, ops, entries, false /*mvcc*/)

	// build keyset and inclusion set.
	keys := make([][]byte, 0, entries)
	for i := 1; i <= entries; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key%05v", i)))
	}
	keys = append(keys, []byte(nil))
	inclusions := []string{"none", "low", "high", "both"}

	validtests := func(bubtindexes []api.Index) {
		// test cases with valid set of parameters
		testcases := make([][]interface{}, 0)
		for i := 0; i < len(keys); i++ {
			for j := i; j < len(keys); j++ {
				for _, incl := range inclusions {
					testcases = append(testcases, []interface{}{
						keys[i], keys[j], incl,
					})
				}
			}
		}
		applymerges(t, bubtindexes, refllrb, false /*mvcc*/, testcases)
	}

	// test cases with random parameters
	randomtests := func(bubtindexes []api.Index) {
		testcases := make([][]interface{}, 0)
		for i := 0; i < n_testcases; i++ {
			testcases = append(testcases, []interface{}{
				keys[rand.Intn(len(keys))],
				keys[rand.Intn(len(keys))],
				inclusions[rand.Intn(len(inclusions))],
			})
		}
		applymerges(t, bubtindexes, refllrb, false /*mvcc*/, testcases)
	}

	//-- flush to bubt with separate datafile
	bubtindexes := []api.Index{}
	for _, index := range indexes {
		bubtindex := buildbubtindex(t, index, true /*datafile*/, false /*mvcc*/)
		bubtindexes = append(bubtindexes, bubtindex)
	}
	validtests(bubtindexes)
	randomtests(bubtindexes)
	destroyindexes(t, bubtindexes, nil)

	//-- flush to bubt without separate datafile
	bubtindexes = []api.Index{}
	for _, index := range indexes {
		bubtindex := buildbubtindex(t, index, false /*datafile*/, false /*mvcc*/)
		bubtindexes = append(bubtindexes, bubtindex)
	}
	validtests(bubtindexes)
	randomtests(bubtindexes)
	destroyindexes(t, bubtindexes, nil)

	//testcases = append(testcases, []interface{}{
	//	[]byte("key00013"),
	//	[]byte("key00019"),
	//	"both",
	//})

	destroyindexes(t, indexes, refllrb)
}

func TestLSMRangeMVCC(t *testing.T) {
	setts := Defaultsettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.deadseqno"] = true
	setts["metadata.vbuuid"] = true
	setts["lsm"] = true
	llrb1 := llrb.NewLLRB("first", setts)
	llrb2 := llrb.NewLLRB("second", setts)
	llrb3 := llrb.NewLLRB("third", setts)
	llrb4 := llrb.NewLLRB("fourth", setts)

	refsetts := setts.Section("")
	refsetts["lsm"] = false
	refsetts["mvcc.enable"] = false
	refllrb := llrb.NewLLRB("reference", refsetts)

	indexes := []api.Index{llrb1, llrb2, llrb3, llrb4}
	entries, ops, n_testcases := 50, 1000, 100000
	buildlsmindexes(t, indexes, refllrb, ops, entries, true /*mvcc*/)

	// build keyset and inclusion set.
	keys := make([][]byte, 0, entries)
	for i := 1; i <= entries; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key%05v", i)))
	}
	keys = append(keys, []byte(nil))
	inclusions := []string{"none", "low", "high", "both"}

	// test cases with valid set of parameters
	validtests := func(bubtindexes []api.Index) {
		testcases := make([][]interface{}, 0)
		for i := 0; i < len(keys); i++ {
			for j := i; j < len(keys); j++ {
				for _, incl := range inclusions {
					testcases = append(testcases, []interface{}{
						keys[i], keys[j], incl,
					})
				}
			}
		}
		applyranges(t, bubtindexes, refllrb, true /*mvcc*/, testcases)
	}

	// test cases with random parameters
	randomtests := func(bubtindexes []api.Index) {
		testcases := make([][]interface{}, 0)
		for i := 0; i < n_testcases; i++ {
			testcases = append(testcases, []interface{}{
				keys[rand.Intn(len(keys))],
				keys[rand.Intn(len(keys))],
				inclusions[rand.Intn(len(inclusions))],
			})
		}
		applyranges(t, bubtindexes, refllrb, true /*mvcc*/, testcases)
	}

	//-- flush to bubt with separate datafile
	bubtindexes := []api.Index{}
	for _, index := range indexes {
		bubtindex := buildbubtindex(t, index, true /*datafile*/, true /*mvcc*/)
		bubtindexes = append(bubtindexes, bubtindex)
	}
	validtests(bubtindexes)
	randomtests(bubtindexes)
	destroyindexes(t, bubtindexes, nil)

	//-- flush to bubt without separate datafile
	bubtindexes = []api.Index{}
	for _, index := range indexes {
		bubtindex := buildbubtindex(t, index, false /*datafile*/, true /*mvcc*/)
		bubtindexes = append(bubtindexes, bubtindex)
	}
	validtests(bubtindexes)
	randomtests(bubtindexes)
	destroyindexes(t, bubtindexes, nil)

	//testcases = append(testcases, []interface{}{
	//	[]byte("key00013"),
	//	[]byte("key00019"),
	//	"both",
	//})

	destroyindexes(t, indexes, refllrb)
}

func TestLSMMergeMVCC(t *testing.T) {
	setts := Defaultsettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.deadseqno"] = true
	setts["metadata.vbuuid"] = true
	setts["lsm"] = true
	llrb1 := llrb.NewLLRB("first", setts)
	llrb2 := llrb.NewLLRB("second", setts)
	llrb3 := llrb.NewLLRB("third", setts)
	llrb4 := llrb.NewLLRB("fourth", setts)

	refsetts := setts.Section("")
	refsetts["mvcc.enable"] = false
	refsetts["lsm"] = false
	refllrb := llrb.NewLLRB("reference", refsetts)

	indexes := []api.Index{llrb1, llrb2, llrb3, llrb4}
	entries, ops, n_testcases := 50, 1000, 100000
	buildlsmindexes(t, indexes, refllrb, ops, entries, true /*mvcc*/)

	// build keyset and inclusion set.
	keys := make([][]byte, 0, entries)
	for i := 1; i <= entries; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key%05v", i)))
	}
	keys = append(keys, []byte(nil))
	inclusions := []string{"none", "low", "high", "both"}

	// test cases with valid set of parameters
	validtests := func(bubtindexes []api.Index) {
		testcases := make([][]interface{}, 0)
		for i := 0; i < len(keys); i++ {
			for j := i; j < len(keys); j++ {
				for _, incl := range inclusions {
					testcases = append(testcases, []interface{}{
						keys[i], keys[j], incl,
					})
				}
			}
		}
		applymerges(t, bubtindexes, refllrb, true /*mvcc*/, testcases)
	}

	// test cases with random parameters
	randomtests := func(bubtindexes []api.Index) {
		testcases := make([][]interface{}, 0)
		for i := 0; i < n_testcases; i++ {
			testcases = append(testcases, []interface{}{
				keys[rand.Intn(len(keys))],
				keys[rand.Intn(len(keys))],
				inclusions[rand.Intn(len(inclusions))],
			})
		}
		applymerges(t, bubtindexes, refllrb, true /*mvcc*/, testcases)
	}

	//-- flush to bubt with separate datafile
	bubtindexes := []api.Index{}
	for _, index := range indexes {
		bubtindex := buildbubtindex(t, index, true /*datafile*/, true /*mvcc*/)
		bubtindexes = append(bubtindexes, bubtindex)
	}
	validtests(bubtindexes)
	randomtests(bubtindexes)
	destroyindexes(t, bubtindexes, nil)

	//-- flush to bubt without separate datafile
	bubtindexes = []api.Index{}
	for _, index := range indexes {
		bubtindex := buildbubtindex(t, index, false /*datafile*/, true /*mvcc*/)
		bubtindexes = append(bubtindexes, bubtindex)
	}
	validtests(bubtindexes)
	randomtests(bubtindexes)
	destroyindexes(t, bubtindexes, nil)

	//testcases = append(testcases, []interface{}{
	//	[]byte("key00013"),
	//	[]byte("key00019"),
	//	"both",
	//})

	destroyindexes(t, indexes, refllrb)
}

func buildlsmindexes(
	t *testing.T, indexes []api.Index, refllrb api.Index,
	ops, entries int, mvcc bool) {

	seqno := uint64(0)

	for i := 0; i < ops; i++ {
		seqno++
		key := []byte(fmt.Sprintf("key%05v", (i%entries)+1))
		value := []byte(fmt.Sprintf("value%05v", i+1))
		rnd := rand.Intn(90)
		index := indexes[rnd%len(indexes)]
		if rnd < 80 { // upsert
			//fmt.Printf("upsert %q %q %q\n", index.ID(), key, value)
			if rnd < 10 {
				value = nil
			}
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
					if err != nil && err.Error() != api.ErrorKeyMissing.Error() {
						t.Error(err)
					} else if nnd != nil {
						nnd.Setvbno(100).SetVbuuid(1000).SetDeadseqno(seqno)
					}
					return true
				})
		}
	}

	// delete missing nodes.
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

		var reader api.IndexReader

		reader = index
		if mvcc {
			snapch := make(chan api.IndexSnapshot, 1)
			if err := index.RSnapshot(snapch, true /*next*/); err != nil {
				t.Fatal(err)
			}
			reader = <-snapch
		}

		reader.Get(
			key,
			func(_ api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if nnd == nil {
					t.Errorf("missing lsm delete for key %s", key)
				}
				if nnd.IsDeleted() == false {
					t.Errorf("expected lsm delete mark %s", nnd.Key())
				}
				return true
			})

		if mvcc {
			reader.(api.IndexSnapshot).Release()
		}

		refllrb.Get(
			key,
			func(_ api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if nnd != nil {
					t.Errorf("expected key missing %s", key)
				}
				return true
			})
	}
}

func buildbubtindex(
	t *testing.T, index api.Index, datafile, mvcc bool) api.Index {

	path := filepath.Join(os.TempDir(), "bubt.lsm.data.bubt-", index.ID())
	os.Remove(path)

	bsetts := Defaultsettings()
	bsetts["datafile"] = datafile
	name := "bubt-" + index.ID()
	bb := NewBubt(name, path, bsetts)

	var reader api.IndexReader
	reader = index
	if mvcc {
		snapch := make(chan api.IndexSnapshot, 1)
		if err := index.RSnapshot(snapch, true /*next*/); err != nil {
			t.Fatal(err)
		}
		reader = <-snapch
	}

	iter := reader.Iterate(nil, nil, "both", false)
	bb.Build(iter, nil)
	iter.Close()
	if mvcc {
		reader.(api.IndexSnapshot).Release()
	}

	store, err := OpenBubtstore(name, path)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func applyranges(
	t *testing.T,
	indexes []api.Index, refllrb api.Index, mvcc bool,
	testcases [][]interface{}) {

	for _, testcase := range testcases {
		low, high := testcase[0].([]byte), testcase[1].([]byte)
		incl := testcase[2].(string)
		//t.Logf("%q %q %q", testcase[0], testcase[1], testcase[2])

		// ascending
		readers, iters := []api.IndexReader{}, []api.IndexIterator{}
		for _, index := range indexes {
			reader := getreader(t, index, mvcc)
			iter := reader.Iterate(low, high, incl, false)
			iters = append(iters, iter)
			readers = append(readers, reader)
		}
		iter := api.LSMRange(false /*reverse*/, iters...)
		refiter := refllrb.Iterate(low, high, incl, false)
		if err := verifylsm(t, iter, refiter, false /*merge*/); err != nil {
			fmsg := "low:%q high:%q incl:%q"
			t.Errorf(fmsg, testcase[0], testcase[1], testcase[2])
			t.Fatal(err)
		}
		if iter != nil {
			iter.Close()
		}
		if mvcc {
			for _, reader := range readers {
				reader.(api.IndexSnapshot).Release()
			}
		}
		if refiter != nil {
			refiter.Close()
		}

		// descending
		readers, iters = []api.IndexReader{}, []api.IndexIterator{}
		for _, index := range indexes {
			reader := getreader(t, index, mvcc)
			iter := reader.Iterate(low, high, incl, true)
			iters = append(iters, iter)
			readers = append(readers, reader)
		}
		iter = api.LSMRange(true /*reverse*/, iters...)
		refiter = refllrb.Iterate(low, high, incl, true)
		if err := verifylsm(t, iter, refiter, false /*merge*/); err != nil {
			fmsg := "low:%q high:%q incl:%q"
			t.Errorf(fmsg, testcase[0], testcase[1], testcase[2])
			t.Fatal(err)
		}
		if iter != nil {
			iter.Close()
		}
		if mvcc {
			for _, reader := range readers {
				reader.(api.IndexSnapshot).Release()
			}

		}
		if refiter != nil {
			refiter.Close()
		}
	}
}

func applymerges(
	t *testing.T,
	indexes []api.Index, refllrb api.Index, mvcc bool,
	testcases [][]interface{}) {

	for _, testcase := range testcases {
		low, high := testcase[0].([]byte), testcase[1].([]byte)
		incl := testcase[2].(string)
		//t.Logf("%q %q %q", testcase[0], testcase[1], testcase[2])

		// ascending
		readers, iters := []api.IndexReader{}, []api.IndexIterator{}
		for _, index := range indexes {
			reader := getreader(t, index, mvcc)
			iter := reader.Iterate(low, high, incl, false)
			iters = append(iters, iter)
			readers = append(readers, reader)
		}
		iter := api.LSMMerge(false /*reverse*/, iters...)
		refiter := refllrb.Iterate(low, high, incl, false)
		if err := verifylsm(t, iter, refiter, true /*merge*/); err != nil {
			fmsg := "low:%q high:%q incl:%q"
			t.Errorf(fmsg, testcase[0], testcase[1], testcase[2])
			t.Fatal(err)
		}
		if iter != nil {
			iter.Close()
		}
		if mvcc {
			for _, reader := range readers {
				reader.(api.IndexSnapshot).Release()
			}
		}
		if refiter != nil {
			refiter.Close()
		}

		// descending
		readers, iters = []api.IndexReader{}, []api.IndexIterator{}
		for _, index := range indexes {
			reader := getreader(t, index, mvcc)
			iter := reader.Iterate(low, high, incl, true)
			iters = append(iters, iter)
			readers = append(readers, reader)
		}
		iter = api.LSMMerge(true /*reverse*/, iters...)
		refiter = refllrb.Iterate(low, high, incl, true)
		if err := verifylsm(t, iter, refiter, true /*merge*/); err != nil {
			fmsg := "low:%q high:%q incl:%q"
			t.Errorf(fmsg, testcase[0], testcase[1], testcase[2])
			t.Fatal(err)
		}
		if iter != nil {
			iter.Close()
		}
		if mvcc {
			for _, reader := range readers {
				reader.(api.IndexSnapshot).Release()
			}
		}
		if refiter != nil {
			refiter.Close()
		}
	}
}

func verifylsm(t *testing.T, iter, refiter api.IndexIterator, merge bool) error {
	if iter == nil && refiter != nil {
		err := fmt.Errorf("iter is nil but refiter is not nil")
		return err
	} else if iter != nil && refiter == nil {
		err := fmt.Errorf("iter is not nil but refiter is nil")
		return err
	} else if iter == nil && refiter == nil {
		return nil
	}
	//for nd := iter.Next(); nd != nil; nd = iter.Next() {
	//	bseqno, dseqno, deleted := nd.Bornseqno(), nd.Deadseqno(), nd.IsDeleted()
	//	fmsg := "Debug lsm %q-%q-%v-%v-%v\n"
	//	t.Logf(fmsg, nd.Key(), nd.Value(), bseqno, dseqno, deleted)
	//}
	//for refnd := refiter.Next(); refnd != nil; refnd = refiter.Next() {
	//	bseqno, dseqno := refnd.Bornseqno(), refnd.Deadseqno()
	//	deleted := refnd.IsDeleted()
	//	fmsg := "Debug ref %q-%q-%v-%v-%v\n"
	//	t.Logf(fmsg, refnd.Key(), refnd.Value(), bseqno, dseqno, deleted)
	//}
	nd, refnd := iter.Next(), refiter.Next()
	for refnd != nil {
		//t.Logf("  nd %q %q\n", nd.Key(), nd.Value())
		//t.Logf("  rd %q %q\n", refnd.Key(), refnd.Value())
		if merge && nd.IsDeleted() {
			nd = iter.Next() // skip this node.
			continue
		}
		if vb, refvb := nd.Vbno(), refnd.Vbno(); vb != refvb {
			err := fmt.Errorf("expected vbno %v, got %v", refvb, vb)
			return err
		}
		vbuuid, refvbuuid := nd.Vbuuid(), refnd.Vbuuid()
		if vbuuid != refvbuuid {
			err := fmt.Errorf("expected vbno %v, got %v", refvbuuid, vbuuid)
			return err
		}
		born, refborn := nd.Bornseqno(), refnd.Bornseqno()
		if born != refborn {
			err := fmt.Errorf("expected bornseqno %v, got %v", refborn, born)
			return err
		}
		dead, refdead := nd.Deadseqno(), refnd.Deadseqno()
		if dead != refdead {
			err := fmt.Errorf("expected dead %v, got %v", refdead, dead)
			return err
		}
		key, refkey := nd.Key(), refnd.Key()
		if bytes.Compare(key, refkey) != 0 {
			err := fmt.Errorf("expected key %q, got %q", refkey, key)
			return err
		}
		value, refvalue := nd.Value(), refnd.Value()
		if bytes.Compare(value, refvalue) != 0 {
			//t.Logf("for key %q %q", refnd.Key(), nd.Key())
			err := fmt.Errorf("expected value %q, got %q", refvalue, value)
			return err
		}
		nd, refnd = iter.Next(), refiter.Next()
	}
	for ; nd != nil; nd = iter.Next() {
		if merge && nd.IsDeleted() {
			continue
		}
		fmsg := "error more nd : %q %q %v\n"
		err := fmt.Errorf(fmsg, nd.Key(), nd.Value(), nd.IsDeleted())
		return err
	}
	ndnil := nd == nil || reflect.ValueOf(nd).IsNil()
	refndnil := refnd == nil || reflect.ValueOf(refnd).IsNil()
	if ndnil == true && refndnil == false {
		err := fmt.Errorf("nd is nil but refnd is not nil")
		return err
	} else if ndnil == false && refndnil == true {
		err := fmt.Errorf("nd is not nil but refnd is nil")
		return err
	}
	return nil
}

func destroyindexes(t *testing.T, indexes []api.Index, refindex api.Index) {
	if refindex != nil {
		indexes = append(indexes, refindex)
	}
	time.Sleep(100 * time.Millisecond)
	for _, index := range indexes {
		if err := index.Destroy(); err != nil {
			t.Fatal(err)
		}
	}
}

func getreader(t *testing.T, index api.Index, mvcc bool) api.IndexReader {
	if mvcc {
		snapch := make(chan api.IndexSnapshot, 1)
		err := index.RSnapshot(snapch, false /*next*/)
		if err != nil {
			t.Fatal(err)
		}
		snapshot := <-snapch
		return snapshot
	}
	return index
}
