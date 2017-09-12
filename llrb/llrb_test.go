package llrb

import "fmt"
import "testing"
import "encoding/json"

import "github.com/prataprc/gostore/lib"

import "github.com/prataprc/gostore/api"
import s "github.com/prataprc/gosettings"
import "github.com/prataprc/golog"

func TestLLRBEmpty(t *testing.T) {
	setts := s.Settings{
		"keycapacity": 10 * 1024 * 1024, "valcapacity": 10 * 1024 * 1024,
	}
	llrb := NewLLRB1("empty", setts)
	defer llrb.Destroy()

	if llrb.ID() != "empty" {
		t.Errorf("unexpected %v", llrb.ID())
	}

	if llrb.Count() != 0 {
		t.Errorf("unexpected %v", llrb.Count())
	}

	// validate statistics
	llrb.Validate()
	stats := llrb.Stats()
	if x := stats["keymemory"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["valmemory"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_count"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_deletes"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_inserts"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_updates"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_reads"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_clones"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_frees"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_nodes"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_txns"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	}

	llrb.Log()
}

func TestLLRBLoad(t *testing.T) {
	var cas uint64

	setts := s.Settings{
		"keycapacity": 10 * 1024 * 1024, "valcapacity": 10 * 1024 * 1024,
	}
	llrb := NewLLRB1("load", setts)
	defer llrb.Destroy()

	if llrb.ID() != "load" {
		t.Errorf("unexpected %v", llrb.ID())
	}

	// load data
	keys := []string{
		"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8",
		"key11", "key12", "key13", "key14", "key15", "key16", "key17", "key18",
	}
	vals := []string{
		"val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8",
		"val11", "val12", "val13", "val14", "val15", "val16", "val17", "val18",
	}
	oldvalue := make([]byte, 1024)
	for i, key := range keys {
		k, v := lib.Str2bytes(key), lib.Str2bytes(vals[i])
		oldvalue, cas = llrb.Set(k, v, oldvalue)
		if len(oldvalue) > 0 {
			t.Errorf("unexpected old value %s", oldvalue)
		} else if cas != uint64(i+1) {
			t.Errorf("expected %v, got %v, key %s", i+1, cas, key)
		}
	}
	// test loaded data
	value := make([]byte, 1024)
	for i, key := range keys {
		if value, cas, _, ok := llrb.Get(lib.Str2bytes(key), value); !ok {
			t.Errorf("expected key %s", key)
		} else if string(value) != vals[i] {
			t.Errorf("expected %s, got %s, key %s", vals[i], value, key)
		} else if cas != uint64(i)+1 {
			t.Errorf("expected %v, got %v, key %s", i+1, cas, key)
		}
	}
	// test set.
	k, v := []byte(keys[0]), []byte("newvalue")
	oldvalue, cas = llrb.Set(k, v, oldvalue)
	if cas != uint64(len(keys)+1) {
		t.Errorf("expected %v, got %v, key %s", len(keys)+1, cas, k)
	} else if string(oldvalue) != vals[0] {
		t.Errorf("expected %s, got %s", vals[0], oldvalue)
	}

	if llrb.Count() != int64(len(keys)) {
		t.Errorf("unexpected %v", llrb.Count())
	}

	// validate
	llrb.Validate()
	stats := llrb.Stats()
	if x := stats["keymemory"].(int64); x != 72 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["valmemory"].(int64); x != 76 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_count"].(int64); x != int64(len(keys)) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_inserts"].(int64); x != int64(len(keys)) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_updates"].(int64); x != 1 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_deletes"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_reads"].(int64); x != int64(len(keys)) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_clones"].(int64); x != 1 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_frees"].(int64); x != 1 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_nodes"].(int64); x != int64(len(keys)) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_txns"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if u := nodeutz(stats); u < 50.0 {
		t.Errorf("unexpected %v", u)
	} else if u := valueutz(stats); u < 50.0 {
		t.Errorf("unexpected %v", u)
	}
}

func TestLLRBLoadLarge(t *testing.T) {
	setts := s.Settings{
		"keycapacity": 100 * 1024 * 1024, "valcapacity": 100 * 1024 * 1024,
	}
	llrb := NewLLRB1("loadlarge", setts)
	defer llrb.Destroy()

	// load data
	n, oldvalue, rkm, rvm := 1000, make([]byte, 1024), 0, 0
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%v", i))
		v := []byte(fmt.Sprintf("val%v", i))
		oldvalue, _ /*cas*/ = llrb.Set(k, v, oldvalue)
		rkm, rvm = rkm+len(k), rvm+len(v)
		llrb.Validate()
	}
	// test loaded data
	value := make([]byte, 1024)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		if value, cas, _, ok := llrb.Get(lib.Str2bytes(key), value); !ok {
			t.Errorf("expected key %s", key)
		} else if string(value) != val {
			t.Errorf("expected %s, got %s, key %s", val, value, key)
		} else if cas != uint64(i)+1 {
			t.Errorf("expected %v, got %v, key %s", i+1, cas, key)
		}
		llrb.Validate()
	}

	if llrb.Count() != int64(n) {
		t.Errorf("unexpected %v", llrb.Count())
	}

	// validate
	stats := llrb.Stats()
	if x := stats["keymemory"].(int64); x != int64(rkm) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["valmemory"].(int64); x != int64(rvm) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_count"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_inserts"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_updates"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_deletes"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_reads"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_clones"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_frees"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_nodes"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_txns"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if u := nodeutz(stats); u < 95.0 {
		t.Errorf("unexpected %v", u)
	} else if u := valueutz(stats); u < 95.0 {
		t.Errorf("unexpected %v", u)
	}
}

func TestLLRBClone(t *testing.T) {
	setts := s.Settings{
		"keycapacity": 100 * 1024 * 1024, "valcapacity": 100 * 1024 * 1024,
	}
	llrb := NewLLRB1("clone", setts)
	defer llrb.Destroy()

	// load data
	n, oldvalue, rkm, rvm := 1000, make([]byte, 1024), 0, 0
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%v", i))
		v := []byte(fmt.Sprintf("val%v", i))
		oldvalue, _ /*cas*/ = llrb.Set(k, v, oldvalue)
		rkm, rvm = rkm+len(k), rvm+len(v)
		llrb.Validate()
	}
	clone := llrb.Clone("loadclone")
	defer clone.Destroy()

	// test loaded data
	value := make([]byte, 1024)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		if value, cas, _, ok := clone.Get(lib.Str2bytes(key), value); !ok {
			t.Errorf("expected key %s", key)
		} else if string(value) != val {
			t.Errorf("expected %s, got %s, key %s", val, value, key)
		} else if cas != uint64(i)+1 {
			t.Errorf("expected %v, got %v, key %s", i+1, cas, key)
		}
		llrb.Validate()
	}

	if clone.Count() != int64(n) {
		t.Errorf("unexpected %v", clone.Count())
	}

	// validate
	stats := clone.Stats()
	if x := stats["keymemory"].(int64); x != int64(rkm) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["valmemory"].(int64); x != int64(rvm) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_count"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_inserts"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_updates"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_deletes"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_reads"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_clones"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_frees"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_nodes"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_txns"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if u := nodeutz(stats); u < 95.0 {
		t.Errorf("unexpected %v", u)
	} else if u := valueutz(stats); u < 95.0 {
		t.Errorf("unexpected %v", u)
	}
}

func TestLLRBSetCAS(t *testing.T) {
	var err error
	var cas uint64

	setts := s.Settings{
		"keycapacity": 100 * 1024 * 1024, "valcapacity": 100 * 1024 * 1024,
	}
	llrb := NewLLRB1("setcas", setts)
	defer llrb.Destroy()

	// load data
	n, oldvalue, rkm, rvm := 1000, make([]byte, 1024), 0, 0
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%v", i))
		v := []byte(fmt.Sprintf("val%v", i))
		if oldvalue, cas, err = llrb.SetCAS(k, v, oldvalue, 0); err != nil {
			t.Error(err)
		} else if len(oldvalue) > 0 {
			t.Errorf("unexpected oldvalue %s", oldvalue)
		} else if cas != uint64(i+1) {
			t.Errorf("expected %v, got %v, key %s", i+1, cas, k)
		}
		rkm, rvm = rkm+len(k), rvm+len(v)
		llrb.Validate()
	}
	// set with cas
	k, v := []byte("key100"), []byte("valu100")
	oldvalue, cas, err = llrb.SetCAS(k, v, oldvalue, 101)
	if err != nil {
		t.Error(err)
	} else if string(oldvalue) != "val100" {
		t.Errorf("unexpected %s", oldvalue)
	} else if cas != uint64(n+1) {
		t.Errorf("expected %v, got %v", n+1, cas)
	}
	rvm = rvm - len(oldvalue) + len(v)
	llrb.Validate()
	// set with invalid cas
	k = []byte("key100")
	oldvalue, cas, err = llrb.SetCAS(k, nil, oldvalue, 100)
	if err.Error() != api.ErrorInvalidCAS.Error() {
		t.Errorf("expected error")
	}
	llrb.Validate()
	// set with cas again
	k, v = []byte("key100"), []byte("value100")
	oldvalue, cas, err = llrb.SetCAS(k, v, oldvalue, uint64(n+1))
	if err != nil {
		t.Error(err)
	} else if string(oldvalue) != "valu100" {
		t.Errorf("unexpected %s", oldvalue)
	} else if cas != uint64(n+2) {
		t.Errorf("expected %v, got %v", n+2, cas)
	}
	rvm = rvm - len(oldvalue) + len(v)
	llrb.Validate()

	// test loaded data
	value := make([]byte, 1024)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		if value, cas, _, ok := llrb.Get(lib.Str2bytes(key), value); !ok {
			t.Errorf("expected key %s", key)
		} else if key == "key100" {
			if string(value) != "value100" {
				t.Errorf("expected %s, got %s, key %s", val, value, key)
			} else if cas != uint64(n+2) {
				t.Errorf("expected %v, got %v, key %s", n+2, cas, key)
			}
		} else {
			if string(value) != val {
				t.Errorf("expected %s, got %s, key %s", val, value, key)
			} else if cas != uint64(i)+1 {
				t.Errorf("expected %v, got %v, key %s", i+1, cas, key)
			}
		}
		llrb.Validate()
	}

	if llrb.Count() != int64(n) {
		t.Errorf("unexpected %v", llrb.Count())
	}

	// validate
	stats := llrb.Stats()
	if x := stats["keymemory"].(int64); x != int64(rkm) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["valmemory"].(int64); x != int64(rvm) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_count"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_inserts"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_updates"].(int64); x != 2 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_deletes"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_reads"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_clones"].(int64); x != 2 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_frees"].(int64); x != 2 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_nodes"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_txns"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if u := nodeutz(stats); u < 95.0 {
		t.Errorf("unexpected %v", u)
	} else if u := valueutz(stats); u < 95.0 {
		t.Errorf("unexpected %v", u)
	}
}

func TestLLRBDelete(t *testing.T) {
	var err error
	var cas uint64

	setts := s.Settings{
		"keycapacity": 100 * 1024 * 1024, "valcapacity": 100 * 1024 * 1024,
	}
	llrb := NewLLRB1("delete", setts)
	defer llrb.Destroy()

	// load data
	n, oldvalue, rkm, rvm := 1000, make([]byte, 1024), 0, 0
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%v", i))
		v := []byte(fmt.Sprintf("val%v", i))
		if oldvalue, cas, err = llrb.SetCAS(k, v, oldvalue, 0); err != nil {
			t.Error(err)
		} else if len(oldvalue) > 0 {
			t.Errorf("unexpected oldvalue %s", oldvalue)
		} else if cas != uint64(i+1) {
			t.Errorf("expected %v, got %v, key %s", i+1, cas, k)
		}
		rkm, rvm = rkm+len(k), rvm+len(v)
		llrb.Validate()
	}
	// delete missing key
	k := []byte("missing")
	oldvalue, cas = llrb.Delete(k, oldvalue, false /*lsm*/)
	if cas != uint64(n+1) {
		t.Errorf("expected %v, got %v", n+1, cas)
	} else if len(oldvalue) > 0 {
		t.Errorf("unexpected %s", oldvalue)
	}
	// mutation: delete a valid key
	k, v := []byte("key100"), []byte("val100")
	oldvalue, cas = llrb.Delete(k, oldvalue, false /*lsm*/)
	if cas != uint64(n+2) {
		t.Errorf("expected %v, got %v", n+2, cas)
	} else if string(oldvalue) != "val100" {
		t.Errorf("unexpected %s", oldvalue)
	}
	rkm, rvm = rkm-len(k), rvm-len(v)
	// test with get
	if oldvalue, cas, delok, ok := llrb.Get(k, oldvalue); ok {
		t.Errorf("unexpected key %s", k)
	} else if delok == true {
		t.Errorf("unexpected true")
	} else if cas != 0 {
		t.Errorf("unexpected cas %v", cas)
	} else if len(oldvalue) > 0 {
		t.Errorf("unexpected %s", oldvalue)
	}
	// mutation: set-cas on deleted key
	k, v = []byte("key100"), []byte("valu100")
	oldvalue, cas, err = llrb.SetCAS(k, v, oldvalue, 0)
	if err != nil {
		t.Error(err)
	} else if cas != uint64(n+3) {
		t.Errorf("expected %v, got %v", n+3, cas)
	}
	rkm, rvm = rkm+len(k), rvm+len(v)
	// mutation: delete with lsm
	k = []byte("key100")
	oldvalue, cas = llrb.Delete(k, oldvalue, true /*lsm*/)
	if cas != uint64(n+4) {
		t.Errorf("expected %v, got %v", n+4, cas)
	} else if string(oldvalue) != "valu100" {
		t.Errorf("unexpected %s", oldvalue)
	}
	rkm, rvm = rkm-len(k), rvm-len(v)
	// test with get lsm deleted key
	if oldvalue, cas, delok, ok := llrb.Get(k, oldvalue); !ok {
		t.Errorf("expected key %s", k)
	} else if delok == false {
		t.Errorf("expected true")
	} else if cas != uint64(n+4) {
		t.Errorf("expected %v, got %v", n+4, cas)
	} else if string(oldvalue) != "valu100" {
		t.Errorf("unexpected %s", oldvalue)
	}
	// mutation: set-cas on deleted key
	k, v = []byte("key100"), []byte("value100")
	oldvalue, cas, err = llrb.SetCAS(k, v, oldvalue, uint64(n+4))
	if err != nil {
		t.Error(err)
	} else if cas != uint64(n+5) {
		t.Errorf("expected %v, got %v", n+5, cas)
	} else if len(oldvalue) > 0 {
		t.Errorf("unexpected %s", oldvalue)
	}
	rkm, rvm = rkm+len(k), rvm+len(v)
	// test with get
	if oldvalue, cas, delok, ok := llrb.Get(k, oldvalue); !ok {
		t.Errorf("expected key %s", k)
	} else if delok == true {
		t.Errorf("unexpected true")
	} else if cas != uint64(n+5) {
		t.Errorf("expected %v, got %v", n+5, cas)
	} else if string(oldvalue) != "value100" {
		t.Errorf("unexpected %s", oldvalue)
	}
	// mutation: delete missing key with lsm
	k = []byte("missing")
	oldvalue, cas = llrb.Delete(k, oldvalue, true /*lsm*/)
	if cas != uint64(n+6) {
		t.Errorf("expected %v, got %v", n+6, cas)
	} else if len(oldvalue) > 0 {
		t.Errorf("unexpected %s", oldvalue)
	}
	rkm = rkm + len(k)
	// test with get
	if oldvalue, cas, delok, ok := llrb.Get(k, oldvalue); !ok {
		t.Errorf("expected key %s", k)
	} else if delok == false {
		t.Errorf("unexpected false")
	} else if cas != uint64(n+6) {
		t.Errorf("expected %v, got %v", n+6, cas)
	} else if len(oldvalue) > 0 {
		t.Errorf("unexpected %s", oldvalue)
	}

	// test loaded data
	value := make([]byte, 1024)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		if value, cas, _, ok := llrb.Get(lib.Str2bytes(key), value); !ok {
			t.Errorf("expected key %s", key)
		} else if key == "key100" {
			if string(value) != "value100" {
				t.Errorf("expected %s, got %s, key %s", val, value, key)
			} else if cas != uint64(n+5) {
				t.Errorf("expected %v, got %v, key %s", n+5, cas, key)
			}
		} else {
			if string(value) != val {
				t.Errorf("expected %s, got %s, key %s", val, value, key)
			} else if cas != uint64(i)+1 {
				t.Errorf("expected %v, got %v, key %s", i+1, cas, key)
			}
		}
		llrb.Validate()
	}

	if llrb.Count() != int64(n+1) {
		t.Errorf("unexpected %v", llrb.Count())
	}

	// validate
	stats := llrb.Stats()
	if x := stats["keymemory"].(int64); x != int64(rkm) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["valmemory"].(int64); x != int64(rvm) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_count"].(int64); x != int64(n+1) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_inserts"].(int64); x != int64(n+2) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_updates"].(int64); x != 1 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_deletes"].(int64); x != 1 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_reads"].(int64); x != int64(n+4) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_clones"].(int64); x != 2 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_frees"].(int64); x != 3 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_nodes"].(int64); x != int64(n+2) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_txns"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if u := nodeutz(stats); u < 95.0 {
		t.Errorf("unexpected %v", u)
	} else if u := valueutz(stats); u < 95.0 {
		t.Errorf("unexpected %v", u)
	}

	// delete all keys
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%v", i))
		oldvalue, cas = llrb.Delete(k, oldvalue, false)
		if cas != uint64(n+6+i+1) {
			t.Errorf("expected %v, got %v, key %s", n+6+i+1, cas, k)
		}
		llrb.Validate()
	}
	k = []byte("missing")
	llrb.Delete(k, oldvalue, false /*lsm*/)

	if llrb.Count() != 0 {
		t.Errorf("unexpected %v", llrb.Count())
	}

	// validate
	stats = llrb.Stats()
	if x := stats["keymemory"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["valmemory"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_count"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_inserts"].(int64); x != int64(n+2) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_updates"].(int64); x != 1 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_deletes"].(int64); x != 1002 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_reads"].(int64); x != int64(n+4) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_clones"].(int64); x != 54 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_frees"].(int64); x != 1056 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_nodes"].(int64); x != int64(n+2) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_txns"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if u := nodeutz(stats); u < 95.0 {
		t.Errorf("unexpected %v", u)
	} else if u := valueutz(stats); u < 95.0 {
		t.Errorf("unexpected %v", u)
	}
	if stats["node.heap"].(int64) != 0 {
		t.Errorf("unexpected %v", stats["node.heap"])
	} else if stats["node.alloc"].(int64) != 0 {
		t.Errorf("unexpected %v", stats["node.alloc"])
	} else if stats["value.alloc"].(int64) != 0 {
		t.Errorf("unexpected %v", stats["value.alloc"])
	} else if stats["value.alloc"].(int64) != 0 {
		t.Errorf("unexpected %v", stats["value.alloc"])
	}
}

func nodeutz(stats map[string]interface{}) float64 {
	heap := stats["node.heap"].(int64)
	used := stats["node.alloc"].(int64)
	return (float64(used) / float64(heap)) * 100
}

func valueutz(stats map[string]interface{}) float64 {
	heap := stats["value.heap"].(int64)
	used := stats["value.alloc"].(int64)
	return float64(used) / float64(heap) * 100
}

func printstats(stats map[string]interface{}) {
	delete(stats, "node.blocks")
	delete(stats, "value.blocks")
	data, err := json.Marshal(stats)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", data)
}

func init() {
	setts := map[string]interface{}{
		"log.level":      "ignore",
		"log.colorfatal": "red",
		"log.colorerror": "hired",
		"log.colorwarn":  "yellow",
	}
	log.SetLogger(nil, setts)
}
