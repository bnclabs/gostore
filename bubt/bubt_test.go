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
		"log.level": "warn",
		"log.file":  "",
	}
	log.SetLogger(nil, config)
}

// TODO:
// 1. add test case with empty iterator.
// 2. add test case with 1 element iterator.
// 3. add test case with 1 element iterator,
//    2 element iterator upto N element iterator.
// 4. test case for Stats and Fullstats
// 5. test case for snapshots, Refer() and Release()

func TestBubtLookup(t *testing.T) {
	indexfile := filepath.Join(os.TempDir(), "ut_indexfile_lookup.bubt")
	datafile := filepath.Join(os.TempDir(), "ut_datafile_lookup.bubt")
	os.Remove(indexfile)
	os.Remove(datafile)
	defer os.Remove(indexfile)
	defer os.Remove(datafile)

	for i := 1; i <= 2000; i++ {
		//t.Logf("trying %v", i)
		llrb := refllrb(i)

		bconfig := Defaultconfig()
		name := fmt.Sprintf("unittest-lookup-%v", i)
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

		zblocksize := bconfig.Int64("zblocksize")
		store, err := OpenBubtstore(name, indexfile, datafile, zblocksize)
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
		} else if s.Count() != int64(i) {
			t.Fatalf("expected %v, got %v", i, s.Count())
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
		store.Destroy()
		if _, err := os.Stat(indexfile); err == nil {
			t.Fatalf("expected %v to be removed", indexfile)
		} else if _, err := os.Stat(datafile); err == nil {
			t.Fatalf("expected %v to be removed", datafile)
		}

		llrb.Destroy()
	}
}

func TestBubtRange(t *testing.T) {
	indexfile := filepath.Join(os.TempDir(), "ut_indexfile_range.bubt")
	datafile := filepath.Join(os.TempDir(), "ut_datafile_range.bubt")
	os.Remove(indexfile)
	os.Remove(datafile)
	defer os.Remove(indexfile)
	defer os.Remove(datafile)

	for i := 1000; i <= 2000; i++ {
		//t.Logf("trying %v", i)
		llrb := refllrb(i)

		bconfig := Defaultconfig()
		name := fmt.Sprintf("unittest-range-%v", i)
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

		zblocksize := bconfig.Int64("zblocksize")
		store, err := OpenBubtstore(name, indexfile, datafile, zblocksize)
		snapch := make(chan api.IndexSnapshot, 1)
		if err := store.RSnapshot(snapch); err != nil {
			t.Fatalf("acquiring snapshot: %v", err)
		}
		s := <-snapch

		if err != nil {
			t.Fatalf("opening bubtstore %v: %v", name, err)
		}

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
		off = i - 1
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
		store.Destroy()
		if _, err := os.Stat(indexfile); err == nil {
			t.Fatalf("expected %v to be removed", indexfile)
		} else if _, err := os.Stat(datafile); err == nil {
			t.Fatalf("expected %v to be removed", datafile)
		}

		llrb.Destroy()
	}
}

func TestBubtIterate(t *testing.T) {
	indexfile := filepath.Join(os.TempDir(), "ut_indexfile_iterate.bubt")
	datafile := filepath.Join(os.TempDir(), "ut_datafile_iterate.bubt")
	os.Remove(indexfile)
	os.Remove(datafile)
	defer os.Remove(indexfile)
	defer os.Remove(datafile)

	for i := 1000; i <= 2000; i++ {
		//t.Logf("trying %v", i)
		llrb := refllrb(i)

		bconfig := Defaultconfig()
		name := fmt.Sprintf("unittest-iteration-%v", i)
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

		zblocksize := bconfig.Int64("zblocksize")
		store, err := OpenBubtstore(name, indexfile, datafile, zblocksize)
		snapch := make(chan api.IndexSnapshot, 1)
		if err := store.RSnapshot(snapch); err != nil {
			t.Fatalf("acquiring snapshot: %v", err)
		}
		s := <-snapch

		if err != nil {
			t.Fatalf("opening bubtstore %v: %v", name, err)
		}

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
		off, nd = i-1, siter.Next()
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
