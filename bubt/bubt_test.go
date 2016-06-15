package bubt

import "testing"
import "math/rand"
import "fmt"
import "os"
import "path/filepath"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/log"
import "github.com/prataprc/storage.go/llrb"

var _ = fmt.Sprintf("dummy")

func init() {
	config := map[string]interface{}{
		"log.level": "error",
		"log.file":  "",
	}
	log.SetLogger(nil, config)
}

func TestEmpty(t *testing.T) {
	indexfile := filepath.Join(os.TempDir(), "ut_indexfile_empty.bubt")
	datafile := filepath.Join(os.TempDir(), "ut_datafile_empty.bubt")
	os.Remove(indexfile)
	os.Remove(datafile)
	defer os.Remove(indexfile)
	defer os.Remove(datafile)

	llrb := refllrb(0)

	bconfig := Defaultconfig()
	name := fmt.Sprintf("unittest-empty-%v", 0)
	bubt := NewBubt(name, indexfile, datafile, bconfig)
	llrbiter := llrb.Iterate(nil, nil, "both", false)
	bubt.Build(llrbiter)
	llrbiter.Close()

	zblocksize := bconfig.Int64("zblocksize")
	_, err := OpenBubtstore(name, indexfile, datafile, zblocksize)
	if err == nil {
		t.Fatalf("expected error")
	}

	llrb.Destroy()
}

func TestMissing(t *testing.T) {
	indexfile := filepath.Join(os.TempDir(), "ut_indexfile_empty.bubt")
	datafile := filepath.Join(os.TempDir(), "ut_datafile_empty.bubt")
	os.Remove(indexfile)
	os.Remove(datafile)
	defer os.Remove(indexfile)
	defer os.Remove(datafile)

	count := 1
	llrb := refllrb(count)

	bconfig := Defaultconfig()
	name := fmt.Sprintf("unittest-empty-%v", count)
	bubt := NewBubt(name, indexfile, datafile, bconfig)
	llrbiter := llrb.Iterate(nil, nil, "both", false)
	bubt.Build(llrbiter)
	llrbiter.Close()

	// gather reference list of keys and values
	keys, vals := make([][]byte, 0), make([][]byte, 0)
	llrb.Range(nil, nil, "both", false, func(nd api.Node) bool {
		keys, vals = append(keys, nd.Key()), append(vals, nd.Value())
		return true
	})
	if llrb.Count() != int64(count) {
		t.Fatalf("expected %v, got %v", count, llrb.Count())
	} else if len(keys) != count {
		t.Fatalf("expected %v, got %v", count, len(keys))
	}

	zblocksize := bconfig.Int64("zblocksize")
	store, err := OpenBubtstore(name, indexfile, datafile, zblocksize)
	if err != nil {
		t.Fatal(err)
	}

	snapch := make(chan api.IndexSnapshot, 1)
	if err := store.RSnapshot(snapch); err != nil {
		t.Fatalf("acquiring snapshot: %v", err)
	}
	s := <-snapch

	if err != nil {
		t.Fatalf("opening bubtstore %v: %v", name, err)
	} else if s.ID() != name {
		t.Fatalf("expected %v, got %v", name, s.ID())
	} else if s.Isactive() == false {
		t.Fatalf("%v inactive", name)
	} else if s.Count() != int64(count) {
		t.Fatalf("expected %v, got %v", 0, s.Count())
	}

	missingkey := []byte("not found")
	if s.Has(missingkey) == true {
		t.Fatalf("expected missing key %v")
	} else if s.Get(missingkey, nil) == true {
		t.Fatalf("expected missing key %v")
	} else if s.Min(nil) == false {
		t.Fatalf("expected minimum key")
	} else if s.Max(nil) == false {
		t.Fatalf("expected maximum key")
	}

	s.Release()

	store.Destroy()
	if _, err := os.Stat(indexfile); err == nil {
		t.Fatalf("expected %v to be removed", indexfile)
	} else if _, err := os.Stat(datafile); err == nil {
		t.Fatalf("expected %v to be removed", datafile)
	}

	llrb.Destroy()
}

func TestLookup(t *testing.T) {
	indexfile := filepath.Join(os.TempDir(), "ut_indexfile_lookup.bubt")
	datafile := filepath.Join(os.TempDir(), "ut_datafile_lookup.bubt")
	name := "unittest-lookup"

	os.Remove(indexfile)
	os.Remove(datafile)
	defer os.Remove(indexfile)
	defer os.Remove(datafile)

	do := func(count int, store *Snapshot, keys, vals [][]byte) {
		snapch := make(chan api.IndexSnapshot, 1)
		err := store.RSnapshot(snapch)
		if err != nil {
			t.Fatalf("acquiring snapshot: %v", err)
		}
		s := <-snapch

		if s.ID() != name {
			t.Fatalf("expected %v, got %v", name, s.ID())
		} else if s.Isactive() == false {
			t.Fatalf("%v inactive", name)
		} else if s.Count() != int64(count) {
			t.Fatalf("expected %v, got %v", count, s.Count())
		}

		for j := 0; j < len(keys); j++ {
			//t.Logf("%v %s", j, string(keys[j]))
			if s.Has(keys[j]) == false {
				t.Fatalf("missing key %v", string(keys[j]))
			}
			s.Get(keys[j], func(nd api.Node) bool {
				if x, y := string(vals[j]), string(nd.Value()); x != y {
					t.Fatalf("expected %v, got %v", y, x)
				}
				return true
			})
		}
		s.Min(func(nd api.Node) bool {
			if x, y := string(keys[0]), string(nd.Key()); x != y {
				t.Fatalf("expected %v, got %v", y, x)
			} else if x, y := string(vals[0]), string(nd.Value()); x != y {
				t.Fatalf("expected %v, got %v", y, x)
			}
			return true
		})
		last := len(keys) - 1
		s.Max(func(nd api.Node) bool {
			if x, y := string(keys[last]), string(nd.Key()); x != y {
				t.Fatalf("expected %v, got %v", y, x)
			} else if x, y := string(vals[last]), string(nd.Value()); x != y {
				t.Fatalf("expected %v, got %v", y, x)
			}
			return true
		})

		s.Release()
	}

	for i := 1; i <= 2000; i++ {
		//t.Logf("trying %v", i)
		llrb := refllrb(i)

		bconfig := Defaultconfig()
		bubt := NewBubt(name, indexfile, datafile, bconfig)
		llrbiter := llrb.Iterate(nil, nil, "both", false)
		bubt.Build(llrbiter)
		llrbiter.Close()

		// gather reference list of keys and values
		keys, vals := make([][]byte, 0), make([][]byte, 0)
		llrb.Range(nil, nil, "both", false, func(nd api.Node) bool {
			keys, vals = append(keys, nd.Key()), append(vals, nd.Value())
			return true
		})
		if llrb.Count() != int64(i) {
			t.Fatalf("expected %v, got %v", i, llrb.Count())
		} else if len(keys) != i {
			t.Fatalf("expected %v, got %v", i, len(keys))
		}

		// with data file
		zblocksize := bconfig.Int64("zblocksize")
		store, err := OpenBubtstore(name, indexfile, datafile, zblocksize)
		if err != nil {
			t.Fatal(err)
		}
		do(i, store, keys, vals)
		store.Destroy()

		// without data file
		bubt = NewBubt(name, indexfile, "", bconfig)
		llrbiter = llrb.Iterate(nil, nil, "both", false)
		bubt.Build(llrbiter)
		llrbiter.Close()
		store, err = OpenBubtstore(name, indexfile, "", zblocksize)
		if err != nil {
			t.Fatal(err)
		}
		do(i, store, keys, vals)
		store.Destroy()

		if _, err := os.Stat(indexfile); err == nil {
			t.Fatalf("expected %v to be removed", indexfile)
		} else if _, err := os.Stat(datafile); err == nil {
			t.Fatalf("expected %v to be removed", datafile)
		}
		llrb.Destroy()
	}
}

func TestRange(t *testing.T) {
	indexfile := filepath.Join(os.TempDir(), "ut_indexfile_range.bubt")
	datafile := filepath.Join(os.TempDir(), "ut_datafile_range.bubt")
	name := "unittest-range"

	os.Remove(indexfile)
	os.Remove(datafile)
	defer os.Remove(indexfile)
	defer os.Remove(datafile)

	do := func(count int, store *Snapshot, keys, vals [][]byte) {
		snapch := make(chan api.IndexSnapshot, 1)
		if err := store.RSnapshot(snapch); err != nil {
			t.Fatalf("acquiring snapshot: %v", err)
		}
		s := <-snapch

		// forward range
		off := 0
		s.Range(nil, nil, "both", false, func(nd api.Node) bool {
			if x, y := string(nd.Key()), string(keys[off]); x != y {
				t.Fatalf("expected %v, got %v", y, x)
			} else if x, y = string(nd.Value()), string(vals[off]); x != y {
				t.Fatalf("expected %v, got %v", y, x)
			}
			off++
			return true
		})
		// backward range
		off = count - 1
		s.Range(nil, nil, "both", true, func(nd api.Node) bool {
			if x, y := string(nd.Key()), string(keys[off]); x != y {
				t.Fatalf("expected %v, got %v", y, x)
			} else if x, y = string(nd.Value()), string(vals[off]); x != y {
				t.Fatalf("expected %v, got %v", y, x)
			}
			off--
			return true
		})

		s.Release()
	}

	for i := 1; i <= 2000; i++ {
		//t.Logf("trying %v", i)
		llrb := refllrb(i)
		keys, vals := make([][]byte, 0), make([][]byte, 0)
		llrb.Range(nil, nil, "both", false, func(nd api.Node) bool {
			keys, vals = append(keys, nd.Key()), append(vals, nd.Value())
			return true
		})

		bconfig := Defaultconfig()
		zblocksize := bconfig.Int64("zblocksize")

		// with datafile
		bubt := NewBubt(name, indexfile, datafile, bconfig)
		llrbiter := llrb.Iterate(nil, nil, "both", false)
		bubt.Build(llrbiter)
		llrbiter.Close()
		store, err := OpenBubtstore(name, indexfile, datafile, zblocksize)
		if err != nil {
			t.Fatal(err)
		}
		do(i, store, keys, vals)
		store.Destroy()
		// without data file
		bubt = NewBubt(name, indexfile, "", bconfig)
		llrbiter = llrb.Iterate(nil, nil, "both", false)
		bubt.Build(llrbiter)
		llrbiter.Close()
		store, err = OpenBubtstore(name, indexfile, "", zblocksize)
		if err != nil {
			t.Fatal(err)
		}
		do(i, store, keys, vals)
		store.Destroy()

		if _, err := os.Stat(indexfile); err == nil {
			t.Fatalf("expected %v to be removed", indexfile)
		} else if _, err := os.Stat(datafile); err == nil {
			t.Fatalf("expected %v to be removed", datafile)
		}

		llrb.Destroy()
	}
}

func TestIterate(t *testing.T) {
	indexfile := filepath.Join(os.TempDir(), "ut_indexfile_iterate.bubt")
	datafile := filepath.Join(os.TempDir(), "ut_datafile_iterate.bubt")
	name := "unittest-iteration"
	os.Remove(indexfile)
	os.Remove(datafile)
	defer os.Remove(indexfile)
	defer os.Remove(datafile)

	do := func(count int, store *Snapshot, keys, vals [][]byte) {
		snapch := make(chan api.IndexSnapshot, 1)
		if err := store.RSnapshot(snapch); err != nil {
			t.Fatalf("acquiring snapshot: %v", err)
		}
		s := <-snapch

		// forward iteration
		siter := s.Iterate(nil, nil, "both", false)
		off, nd := 0, siter.Next()
		for nd != nil {
			if x, y := string(nd.Key()), string(keys[off]); x != y {
				t.Fatalf("expected %v, got %v", y, x)
			} else if x, y = string(nd.Value()), string(vals[off]); x != y {
				t.Fatalf("expected %v, got %v", y, x)
			}
			nd = siter.Next()
			off++
		}
		siter.Close()
		// backward iteration
		siter = s.Iterate(nil, nil, "both", true)
		off, nd = count-1, siter.Next()
		for nd != nil {
			if x, y := string(nd.Key()), string(keys[off]); x != y {
				t.Fatalf("expected %v, got %v", y, x)
			} else if x, y = string(nd.Value()), string(vals[off]); x != y {
				t.Fatalf("expected %v, got %v", y, x)
			}
			nd = siter.Next()
			off--
		}
		siter.Close()

		s.Release()
	}

	for i := 1; i <= 2000; i++ {
		//t.Logf("trying %v", i)
		llrb := refllrb(i)
		keys, vals := make([][]byte, 0), make([][]byte, 0)
		llrb.Range(nil, nil, "both", false, func(nd api.Node) bool {
			keys, vals = append(keys, nd.Key()), append(vals, nd.Value())
			return true
		})

		bconfig := Defaultconfig()
		zblocksize := bconfig.Int64("zblocksize")

		// with datafile
		bubt := NewBubt(name, indexfile, datafile, bconfig)
		llrbiter := llrb.Iterate(nil, nil, "both", false)
		bubt.Build(llrbiter)
		llrbiter.Close()
		store, err := OpenBubtstore(name, indexfile, datafile, zblocksize)
		if err != nil {
			t.Fatal(err)
		}
		do(i, store, keys, vals)
		store.Destroy()
		// without datafile
		bubt = NewBubt(name, indexfile, "", bconfig)
		llrbiter = llrb.Iterate(nil, nil, "both", false)
		bubt.Build(llrbiter)
		llrbiter.Close()
		store, err = OpenBubtstore(name, indexfile, datafile, zblocksize)
		if err != nil {
			t.Fatal(err)
		}
		do(i, store, keys, vals)
		store.Destroy()

		if _, err := os.Stat(indexfile); err == nil {
			t.Fatalf("expected %v to be removed", indexfile)
		} else if _, err := os.Stat(datafile); err == nil {
			t.Fatalf("expected %v to be removed", datafile)
		}

		llrb.Destroy()
	}
}

func makekeyvalue(key, value []byte) ([]byte, []byte) {
	if key != nil {
		for i := 0; i < len(key); i++ {
			x := rand.Intn(26)
			key[i] = byte(97 + (x % 26))
		}
	}
	if value != nil {
		for i := 0; i < len(value); i++ {
			x := rand.Intn(26)
			value[i] = byte(97 + (x % 26))
		}
	}
	return key, value
}

func refllrb(count int) *llrb.LLRB {
	lconfig := llrb.Defaultconfig()
	lconfig["metadata.bornseqno"] = true
	lconfig["metadata.deadseqno"] = true
	lconfig["metadata.vbuuid"] = true
	lconfig["metadata.fpos"] = true
	llrb := llrb.NewLLRB("bubttest", lconfig)

	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	// insert 1 items
	key, value := make([]byte, 100), make([]byte, 100)
	for i := 0; i < count; i++ {
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, newnd, oldnd api.Node) {
				if oldnd != nil {
					panic(fmt.Errorf("expected nil"))
				} else if x := index.Count(); x != int64(i+1) {
					panic(fmt.Errorf("expected %v, got %v", i, x))
				}
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				if i%3 == 0 {
					seqno++
					newnd.SetDeadseqno(seqno)
				}
			})
		seqno++
	}
	return llrb
}
