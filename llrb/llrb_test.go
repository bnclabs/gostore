package llrb

import "io"
import "fmt"
import "bytes"
import "testing"
import "io/ioutil"
import "encoding/json"
import "encoding/binary"

import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/api"

func TestLLRBEmpty(t *testing.T) {
	llrb := NewLLRB("empty", Defaultsettings())
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

	setts := Defaultsettings()
	setts["memcapacity"] = 1 * 1024 * 1024
	llrb := NewLLRB("load", setts)
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
	// test set with nil for oldvalue.
	nilvalue := []byte(nil)
	k, v = []byte(keys[0]), []byte("newvalue1")
	nilvalue, cas = llrb.Set(k, v, nil)
	if cas != uint64(len(keys)+2) {
		t.Errorf("expected %v, got %v, key %s", len(keys)+1, cas, k)
	} else if len(nilvalue) != 0 {
		t.Errorf("unexpected %s", nilvalue)
	}
	// test set with value nil.
	k, v = []byte(keys[0]), nil
	oldvalue, cas = llrb.Set(k, v, oldvalue)
	if cas != uint64(len(keys)+3) {
		t.Errorf("expected %v, got %v, key %s", len(keys)+1, cas, k)
	} else if string(oldvalue) != "newvalue1" {
		t.Errorf("unexpected %q", oldvalue)
	}
	// test set with oldvalue nil.
	k, v = []byte(keys[0]), []byte("newvalue2")
	oldvalue, cas = llrb.Set(k, v, nil)
	if cas != uint64(len(keys)+4) {
		t.Errorf("expected %v, got %v, key %s", len(keys)+1, cas, k)
	} else if len(oldvalue) != 0 {
		t.Errorf("unexpected %s", oldvalue)
	}
	if value, cas, deleted, ok := llrb.Get(k, value); ok == false {
		t.Errorf("unexpected false")
	} else if deleted == true {
		t.Errorf("expected key")
	} else if cas != uint64(len(keys)+4) {
		t.Errorf("unexpected %v", cas)
	} else if string(value) != "newvalue2" {
		t.Errorf("unexpected value %s", value)
	}

	if llrb.Count() != int64(len(keys)) {
		t.Errorf("unexpected %v", llrb.Count())
	}

	// validate
	llrb.Validate()
	stats := llrb.Stats()
	if x := stats["keymemory"].(int64); x != 72 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["valmemory"].(int64); x != 77 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_count"].(int64); x != int64(len(keys)) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_inserts"].(int64); x != int64(len(keys)) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_updates"].(int64); x != 4 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_deletes"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_clones"].(int64); x != 4 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_frees"].(int64); x != 4 {
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

func TestLoadLLRB(t *testing.T) {
	llrb1 := NewLLRB("load", Defaultsettings())
	defer llrb1.Destroy()

	// load data
	keys := []string{
		"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8",
		"key11", "key12", "key13", "key14", "key15", "key16", "key17", "key18",
	}
	vals := []string{
		"val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8",
		"val11", "val12", "val13", "val14", "val15", "val16", "val17", "val18",
	}
	for i, key := range keys {
		k, v := lib.Str2bytes(key), lib.Str2bytes(vals[i])
		llrb1.Set(k, v, nil)
	}
	llrb1.Delete([]byte("key2"), nil, true /*lsm*/)

	llrb2 := LoadLLRB("warmup", Defaultsettings(), llrb1.Scan())
	defer llrb2.Destroy()
	llrb2.Setseqno(llrb1.Getseqno())

	iter1, iter2 := llrb1.Scan(), llrb2.Scan()

	key1, val1, seqno1, del1, err1 := iter1(false /*close*/)
	key2, val2, seqno2, del2, err2 := iter2(false /*close*/)
	for err1 == nil {
		if bytes.Compare(key1, key2) != 0 {
			t.Errorf("expected %q, got %q", key1, key2)
		} else if del1 != del2 {
			t.Errorf("expected %v, got %v", del1, del2)
		} else if del1 == false && bytes.Compare(val1, val2) != 0 {
			t.Errorf("expected %q, got %q", val1, val2)
		} else if seqno1 != seqno2 {
			t.Errorf("expected %v, got %v", seqno1, seqno2)
		} else if err2 != nil {
			t.Errorf("for %q, unexpected %v", key1, err2)
		}
		key1, val1, seqno1, del1, err1 = iter1(false /*close*/)
		key2, val2, seqno2, del2, err2 = iter2(false /*close*/)
	}

	llrb1.Validate()
	llrb2.Validate()
}

func TestLLRBDotdump(t *testing.T) {
	llrb := NewLLRB("load", Defaultsettings())
	defer llrb.Destroy()

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
		llrb.Set(k, v, oldvalue)
	}

	buf := bytes.NewBuffer(nil)
	llrb.Dotdump(buf)
	data, err := ioutil.ReadFile("testdata/llrbload.dot")
	if err != nil {
		t.Error(err)
	}
	if out := append(buf.Bytes(), '\n'); bytes.Compare(data, out) != 0 {
		t.Errorf("mismatch in dotdump")
		t.Errorf("%s", out)
		t.Errorf("%s", data)
	}
}

func TestLLRBLoadLarge(t *testing.T) {
	setts := Defaultsettings()
	setts["memcapacity"] = 1 * 1024 * 1024
	llrb := NewLLRB("loadlarge", setts)
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
	setts := Defaultsettings()
	setts["memcapacity"] = 1 * 1024 * 1024
	llrb := NewLLRB("clone", setts)
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

	setts := Defaultsettings()
	setts["memcapacity"] = 1 * 1024 * 1024
	llrb := NewLLRB("setcas", setts)
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
	k = []byte("key100")
	oldvalue, cas = llrb.Delete(k, oldvalue, true /*lsm*/)
	if string(oldvalue) != "valu100" {
		t.Errorf("unexpected %s", oldvalue)
	} else if cas != uint64(n+2) {
		t.Errorf("expected %v, got %v", n+2, cas)
	}
	rvm = rvm - len(oldvalue)
	llrb.Validate()
	// set with mismatch cas for deleted key.
	k, v = []byte("key100"), []byte("value100")
	oldvalue, cas, err = llrb.SetCAS(k, v, oldvalue, 100)
	if err.Error() != api.ErrorInvalidCAS.Error() {
		t.Errorf("expected error")
	}
	llrb.Validate()
	// set with mismatch cas and oldvalue as nil.
	nilvalue := []byte(nil)
	k, v = []byte("key100"), []byte("value100")
	nilvalue, cas, err = llrb.SetCAS(k, v, nil, 100)
	if err.Error() != api.ErrorInvalidCAS.Error() {
		t.Errorf("expected error")
	} else if nilvalue != nil {
		t.Errorf("unexpected %s", nilvalue)
	}
	llrb.Validate()
	// set with cas and value nil.
	k, v = []byte("key100"), nil
	oldvalue, cas, err = llrb.SetCAS(k, v, oldvalue, 1002)
	if err != nil {
		t.Error(err)
	} else if cas != 1003 {
		t.Errorf("unexpected %v", cas)
	} else if string(oldvalue) != "" {
		t.Errorf("unexpected %s", oldvalue)
	}
	llrb.Validate()
	// set with cas and oldvalue nil.
	k, v = []byte("key100"), []byte("value100")
	nilvalue, cas, err = llrb.SetCAS(k, v, nil, 1003)
	if err != nil {
		t.Error(err)
	} else if cas != 1004 {
		t.Errorf("unexpected %v", cas)
	} else if nilvalue != nil {
		t.Errorf("unexpected %s", nilvalue)
	}
	rvm = rvm + len(v)
	llrb.Validate()
	// set with cas and oldvalue nil.
	k, v = []byte("missing"), []byte("value100")
	oldvalue, cas, err = llrb.SetCAS(k, v, oldvalue, 1003)
	if err.Error() != api.ErrorInvalidCAS.Error() {
		t.Errorf("unexpected %v", err)
	}
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
			} else if cas != uint64(n+4) {
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
		t.Errorf("unexpected %v, %v", x, rvm)
	} else if x := stats["n_count"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_inserts"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_updates"].(int64); x != 3 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_deletes"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_clones"].(int64); x != 3 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_frees"].(int64); x != 3 {
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

	setts := Defaultsettings()
	setts["memcapacity"] = 1 * 1024 * 1024
	llrb := NewLLRB("delete", setts)
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
	} else if x := stats["n_clones"].(int64); x != 54 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_frees"].(int64); x != 1056 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_nodes"].(int64); x != int64(n+2) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_txns"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if u := nodeutz(stats); u != 0 {
		t.Errorf("unexpected %v", u)
	} else if u := valueutz(stats); u != 0 {
		t.Errorf("unexpected %v", u)
	}
	if stats["node.heap"].(int64) != 65280 {
		t.Errorf("unexpected %v", stats["node.heap"])
	} else if stats["node.alloc"].(int64) != 0 {
		t.Errorf("unexpected %v", stats["node.alloc"])
	} else if stats["value.alloc"].(int64) != 0 {
		t.Errorf("unexpected %v", stats["value.alloc"])
	} else if stats["value.alloc"].(int64) != 0 {
		t.Errorf("unexpected %v", stats["value.alloc"])
	}
}

func TestLLRBTxn(t *testing.T) {
	llrb := NewLLRB("txn", Defaultsettings())
	defer llrb.Destroy()

	// First transaction
	txn := llrb.BeginTxn(0x1234)
	if txn.ID() != 0x1234 {
		t.Errorf("unexpected %v", txn.ID())
	}
	// set initial values
	key, value, oldvalue := []byte("plumless"), []byte("value1"), []byte{}
	oldvalue = txn.Set(key, value, oldvalue)
	if len(oldvalue) > 0 {
		t.Errorf("unexpected %s", oldvalue)
	}
	key, value = []byte("buckeroo"), []byte("value2")
	oldvalue = txn.Set(key, value, oldvalue)
	if len(oldvalue) > 0 {
		t.Errorf("unexpected %s", oldvalue)
	}
	// get entries
	key = []byte("plumless")
	oldvalue, cas, deleted, ok := txn.Get(key, oldvalue)
	if ok == false {
		t.Errorf("unexpected false")
	} else if deleted == true {
		t.Errorf("unexpected deleted")
	} else if string(oldvalue) != "value1" {
		t.Errorf("unexpected %s", oldvalue)
	} else if cas != 0 {
		t.Errorf("unexpected %v", cas)
	}
	txn.Commit()
	// verify first transaction
	key, value = []byte("buckeroo"), []byte{}
	value, _, deleted, ok = llrb.Get(key, value)
	if ok == false {
		t.Errorf("unexpected false")
	} else if deleted == true {
		t.Errorf("unexpected delete")
	} else if string(value) != "value2" {
		t.Errorf("unexpected %s", value)
	}

	// Second transaction
	txn = llrb.BeginTxn(0x12345)
	key, value, oldvalue = []byte("plumless"), []byte("value11"), []byte{}
	oldvalue = txn.Set(key, value, oldvalue)
	if string(oldvalue) != "value1" {
		t.Errorf("unexpected %s", oldvalue)
	}
	oldvalue = txn.Delete(key, oldvalue, false)
	if string(oldvalue) != "value11" {
		t.Errorf("unexpected %s", oldvalue)
	}
	value = []byte("value111")
	oldvalue = txn.Set(key, value, oldvalue)
	if string(oldvalue) != "" {
		t.Errorf("unexpected %s", oldvalue)
	}
	oldvalue = txn.Delete(key, oldvalue, true)
	if string(oldvalue) != "value111" {
		t.Errorf("unexpected %s", oldvalue)
	}
	value, cas, deleted, ok = txn.Get(key, []byte{})
	if deleted == false {
		t.Errorf("expected as deleted")
	} else if ok == false {
		t.Errorf("expected key")
	} else if len(value) > 0 {
		t.Errorf("unexpected %s", value)
	} else if cas != 1 {
		t.Errorf("unexpected %v", cas)
	}
	txn.Commit()
	// verify second transaction
	key, value = []byte("plumless"), []byte{}
	value, _, deleted, ok = llrb.Get(key, value)
	if ok == false {
		t.Errorf("unexpected false")
	} else if deleted == false {
		t.Errorf("expected delete")
	} else if string(value) != "value1" {
		t.Errorf("unexpected %s", value)
	}

	// third transaction abort
	txn = llrb.BeginTxn(0)
	// set initial values
	key, value = []byte("plumless"), []byte("aborted")
	txn.Set(key, value, nil)
	txn.Abort()
	// get entries
	key = []byte("plumless")
	value, _, _, _ = txn.Get(key, value)
	if string(value) != "value1" {
		t.Errorf("unexpected %s", value)
	}
}

func TestLLRBView(t *testing.T) {
	llrb := NewLLRB("view", Defaultsettings())
	defer llrb.Destroy()

	keys := []string{
		"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8",
		"key11", "key12", "key13", "key14", "key15", "key16", "key17", "key18",
	}
	vals := []string{
		"val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8",
		"val11", "val12", "val13", "val14", "val15", "val16", "val17", "val18",
	}
	for i, key := range keys {
		k, v := lib.Str2bytes(key), lib.Str2bytes(vals[i])
		llrb.Set(k, v, nil)
	}

	view := llrb.View(0x1234)
	defer view.Abort()

	if view.ID() != 0x1234 {
		t.Errorf("unexpected %v", view.ID())
	}
	var deleted, ok bool
	var cas uint64
	value := []byte{}
	for i, key := range keys {
		k := lib.Str2bytes(key)
		value, cas, deleted, ok = view.Get(k, value)
		if string(value) != vals[i] {
			t.Errorf("for %v expected %v, got %v", i, vals[i], value)
		} else if deleted == true {
			t.Errorf("unexpected deleted")
		} else if ok == false {
			t.Errorf("key %s missing", k)
		} else if cas != uint64(i)+1 {
			t.Errorf("expected %v, got %v", i+1, cas)
		}
	}
}

func TestLLRBTxnCursor1(t *testing.T) {
	llrb := NewLLRB("view", Defaultsettings())
	defer llrb.Destroy()

	keys := []string{
		"key1", "key11", "key12", "key13", "key14", "key15", "key16",
		"key17", "key18",
		"key2", "key3", "key4", "key5", "key6", "key7", "key8",
	}
	vals := []string{
		"val1", "val11", "val12", "val13", "val14", "val15", "val16",
		"val17", "val18",
		"val2", "val3", "val4", "val5", "val6", "val7", "val8",
	}
	for i, key := range keys {
		k, v := lib.Str2bytes(key), lib.Str2bytes(vals[i])
		llrb.Set(k, v, nil)
	}
	llrb.Delete([]byte(keys[15]), nil, true /*lsm*/)

	// llrb.getroot().ptrdump(" ")

	txn := llrb.BeginTxn(0x1234)
	for i, key := range keys {
		cur, _ := txn.OpenCursor([]byte(key))
		testgetnext(t, cur, i, keys, vals)
		if k, _, _, _ := cur.GetNext(); k != nil {
			t.Errorf("unexpected %s", k)
		}
		cur, _ = txn.OpenCursor([]byte(key))
		testynext(t, cur, i, keys, vals)
		if k, _ := cur.Key(); k != nil {
			t.Errorf("unexpected %s", k)
		} else if v := cur.Value(); v != nil {
			t.Errorf("unexpected %s", v)
		}
	}

	// full table scan
	cur, _ := txn.OpenCursor(nil)
	testgetnext(t, cur, 0, keys, vals)
	if k, _, _, _ := cur.GetNext(); k != nil {
		t.Errorf("unexpected %s", k)
	}
	cur, _ = txn.OpenCursor(nil)
	testynext(t, cur, 0, keys, vals)
	if k, _ := cur.Key(); k != nil {
		t.Errorf("unexpected %s", k)
	} else if v := cur.Value(); v != nil {
		t.Errorf("unexpected %s", v)
	}

	cur, _ = txn.OpenCursor([]byte(keys[0]))
	cur.Delcursor(true /*lsm*/)
	cur.Delete([]byte(keys[1]), nil, true /*lsm*/)
	value := []byte("newvalue")
	cur.Set([]byte(keys[2]), value, nil)
	txn.Commit()

	value, _, deleted, ok := llrb.Get([]byte(keys[0]), []byte{})
	if deleted == false {
		t.Errorf("expected deleted")
	} else if ok == false {
		t.Errorf("expected key")
	} else if string(value) != vals[0] {
		t.Errorf("expected %s, got %s", vals[0], value)
	}
	value, _, deleted, ok = llrb.Get([]byte(keys[1]), []byte{})
	if deleted == false {
		t.Errorf("expected deleted")
	} else if ok == false {
		t.Errorf("expected key")
	} else if string(value) != vals[1] {
		t.Errorf("expected %s, got %s", vals[1], value)
	}
	value, _, deleted, ok = llrb.Get([]byte(keys[2]), []byte{})
	if deleted == true {
		t.Errorf("unexpected deleted")
	} else if ok == false {
		t.Errorf("expected key")
	} else if string(value) != "newvalue" {
		t.Errorf("unexpected %s", value)
	}
}

func TestLLRBTxnCursor2(t *testing.T) {
	mi, _ := makeLLRB(10000)
	defer mi.Destroy()

	key := []byte("key11730000000")
	id := uint64(0x12345699)
	mview := mi.View(id)
	mcur, _ := mview.OpenCursor(key)
	key, value, seqno, del, err := mcur.YNext(false /*fin*/)
	if bytes.Compare(key, []byte("key1174")) != 0 {
		t.Errorf("expected %q, got %q", "key1174", key)
	} else if bytes.Compare(value, []byte("val1174")) != 0 {
		t.Errorf("for %q expected %q, got %q", key, "val1174", value)
	} else if seqno != 1293 {
		t.Errorf("expected %q, got %v", 1293, seqno)
	} else if del != false {
		t.Errorf("expected %v, got %v", false, del)
	} else if err != nil {
		t.Errorf("unexpected %v", err)
	}
	mview.Abort()
}

func TestLLRBViewCursor(t *testing.T) {
	llrb := NewLLRB("view", Defaultsettings())
	defer llrb.Destroy()

	keys := []string{
		"key1", "key11", "key12", "key13", "key14", "key15", "key16",
		"key17", "key18",
		"key2", "key3", "key4", "key5", "key6", "key7", "key8",
	}
	vals := []string{
		"val1", "val11", "val12", "val13", "val14", "val15", "val16",
		"val17", "val18",
		"val2", "val3", "val4", "val5", "val6", "val7", "val8",
	}
	for i, key := range keys {
		k, v := lib.Str2bytes(key), lib.Str2bytes(vals[i])
		llrb.Set(k, v, nil)
	}
	llrb.Delete([]byte(keys[15]), nil, true /*lsm*/)

	// llrb.getroot().ptrdump(" ")

	for i, key := range keys {
		view := llrb.View(0x1234 + uint64(i))
		cur, _ := view.OpenCursor([]byte(key))
		testgetnext(t, cur, i, keys, vals)
		view.Abort()
		view = llrb.View(0)
		cur, _ = view.OpenCursor([]byte(key))
		testynext(t, cur, i, keys, vals)
		view.Abort()
	}

	// full table scan
	view := llrb.View(0)
	cur, _ := view.OpenCursor(nil)
	testgetnext(t, cur, 0, keys, vals)
	if k, _, _, _ := cur.GetNext(); k != nil {
		t.Errorf("unexpected %s", k)
	}
	cur, _ = view.OpenCursor(nil)
	testynext(t, cur, 0, keys, vals)
	if k, _ := cur.Key(); k != nil {
		t.Errorf("unexpected %s", k)
	} else if v := cur.Value(); v != nil {
		t.Errorf("unexpected %s", v)
	}
	view.Abort()

	txn := llrb.BeginTxn(0x12345)
	cur, _ = txn.OpenCursor([]byte(keys[0]))
	cur.Delcursor(true /*lsm*/)
	cur.Delete([]byte(keys[1]), nil, true /*lsm*/)
	value := []byte("newvalue")
	cur.Set([]byte(keys[2]), value, nil)
	txn.Commit()

	value, _, deleted, ok := llrb.Get([]byte(keys[0]), []byte{})
	if deleted == false {
		t.Errorf("expected deleted")
	} else if ok == false {
		t.Errorf("expected key")
	} else if string(value) != vals[0] {
		t.Errorf("expected %s, got %s", vals[0], value)
	}
	value, _, deleted, ok = llrb.Get([]byte(keys[1]), []byte{})
	if deleted == false {
		t.Errorf("expected deleted")
	} else if ok == false {
		t.Errorf("expected key")
	} else if string(value) != vals[1] {
		t.Errorf("expected %s, got %s", vals[1], value)
	}
	value, _, deleted, ok = llrb.Get([]byte(keys[2]), []byte{})
	if deleted == true {
		t.Errorf("unexpected deleted")
	} else if ok == false {
		t.Errorf("expected key")
	} else if string(value) != "newvalue" {
		t.Errorf("unexpected %s", value)
	}
}

func TestLLRBScan(t *testing.T) {
	load := func(n int, llrb *LLRB) {
		for i := 0; i < n; i++ {
			k := []byte(fmt.Sprintf("key%v", i))
			v := []byte(fmt.Sprintf("val%v", i))
			llrb.Set(k, v, nil)
			llrb.Validate()
		}
	}

	compare := func(n int, llrb *LLRB) {
		view := llrb.View(0)
		defer view.Abort()

		count := 0
		cur, _ := view.OpenCursor(nil)
		iter := llrb.Scan()

		refkey, refval, refseqno, refdeleted, referr := cur.YNext(false /*fin*/)
		key, val, seqno, deleted, err := iter(false /*close*/)
		for referr == nil && referr == nil {
			count++
			orgkey := []byte(fmt.Sprintf("key%v", count))
			orgval := []byte(fmt.Sprintf("val%v", count))
			if bytes.Compare(orgkey, key) != 0 {
				t.Errorf("expected %q, got %q", orgkey, key)
			} else if bytes.Compare(orgval, val) != 0 {
				t.Errorf("for %q, expected %q, got %q", key, orgval, val)
			} else if uint64(count) != seqno {
				t.Errorf("for %q, expected %v, got %v", key, count, seqno)
			}

			//t.Logf("iter %q iter:%q", refkey, key)
			if bytes.Compare(key, refkey) != 0 {
				t.Errorf("expected %q, got %q", refkey, key)
			} else if bytes.Compare(val, refval) != 0 {
				t.Errorf("expected %s, got %s", refval, val)
			} else if seqno != refseqno {
				t.Errorf("expected %v, got %v", refseqno, seqno)
			} else if deleted != refdeleted {
				t.Errorf("expected %v, got %v", refdeleted, deleted)
			}
			refkey, refval, refseqno, refdeleted, referr = cur.YNext(false /*fin*/)
			key, val, seqno, deleted, err = iter(false /*close*/)
		}
		if err != io.EOF || referr != io.EOF {
			t.Errorf("expected nil %v, %v", referr, err)
		}
	}

	// load data
	for i := 0; i < 10000; i++ {
		llrb := NewLLRB("scan", Defaultsettings())
		load(i, llrb)
		compare(i, llrb)
		llrb.Destroy()
	}

}

func BenchmarkLLRBCount(b *testing.B) {
	llrb := makeBenchLLRB(1000)
	defer llrb.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		llrb.Count()
	}
}

func BenchmarkLLRBSet(b *testing.B) {
	var scratch [8]byte

	llrb := NewLLRB("bench", Defaultsettings())
	defer llrb.Destroy()

	b.ResetTimer()
	k, v := []byte("key000000000000"), []byte("val00000000000000")
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(scratch[:], uint64(i+1))
		key, val := append(k[:3], scratch[:]...), append(v[:3], scratch[:]...)
		llrb.Set(key, val, nil)
	}
}

func BenchmarkLLRBCAS(b *testing.B) {
	var scratch [8]byte

	llrb := NewLLRB("bench", Defaultsettings())
	defer llrb.Destroy()

	b.ResetTimer()
	k, v := []byte("key000000000000"), []byte("val00000000000000")
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(scratch[:], uint64(i+1))
		key, val := append(k[:3], scratch[:]...), append(v[:3], scratch[:]...)
		llrb.SetCAS(key, val, nil, 0)
	}
}

func BenchmarkLLRBDel(b *testing.B) {
	var scratch [8]byte

	llrb := makeBenchLLRB(b.N)
	defer llrb.Destroy()

	b.ResetTimer()
	k := []byte("key000000000000")
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(scratch[:], uint64(i+1))
		key := append(k[:3], scratch[:]...)
		llrb.Delete(key, nil, false /*lsm*/)
	}
}

func BenchmarkLLRBDelLSM(b *testing.B) {
	var scratch [8]byte

	llrb := makeBenchLLRB(b.N)
	defer llrb.Destroy()

	b.ResetTimer()
	k := []byte("key000000000000")
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(scratch[:], uint64(i+1))
		key := append(k[:3], scratch[:]...)
		llrb.Delete(key, nil, true /*lsm*/)
	}
}

func BenchmarkLLRBGet(b *testing.B) {
	var scratch [8]byte

	llrb := makeBenchLLRB(b.N)
	defer llrb.Destroy()

	b.ResetTimer()
	k := []byte("key000000000000")
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(scratch[:], uint64(i+1))
		key := append(k[:3], scratch[:]...)
		llrb.Get(key, nil)
	}
}

func BenchmarkLLRBTxn(b *testing.B) {
	llrb := makeBenchLLRB(1000)
	defer llrb.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := llrb.BeginTxn(0)
		txn.Commit()
	}
}

func BenchmarkLLRBClone(b *testing.B) {
	llrb := makeBenchLLRB(b.N)
	defer llrb.Destroy()

	b.ResetTimer()
	llrb.Clone("benchclone")
}

func BenchmarkLLRBView(b *testing.B) {
	llrb := makeBenchLLRB(1000)
	defer llrb.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := llrb.View(0)
		txn.Abort()
	}
}

func BenchmarkLLRBOpenCur(b *testing.B) {
	llrb := makeBenchLLRB(1000)
	defer llrb.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := llrb.BeginTxn(0)
		txn.OpenCursor([]byte("key500"))
		txn.Commit()
	}
}

func BenchmarkLLRBGetNext(b *testing.B) {
	llrb := makeBenchLLRB(b.N)
	defer llrb.Destroy()

	txn := llrb.BeginTxn(0)
	cur, _ := txn.OpenCursor(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur.GetNext()
	}
	txn.Commit()
}

func BenchmarkLLRBYNext(b *testing.B) {
	llrb := makeBenchLLRB(b.N)
	defer llrb.Destroy()

	view := llrb.BeginTxn(0)
	cur, _ := view.OpenCursor(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur.YNext(false /*fin*/)
	}
	view.Abort()
}

func BenchmarkLLRBScan(b *testing.B) {
	llrb := makeBenchLLRB(b.N)
	defer llrb.Destroy()

	scan := llrb.Scan()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan(false /*close*/)
	}
}

func makeBenchLLRB(n int) *LLRB {
	var scratch [8]byte

	llrb := NewLLRB("bench", Defaultsettings())
	k, v := []byte("key000000000000"), []byte("val00000000000000")
	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(scratch[:], uint64(i+1))
		key, val := append(k[:3], scratch[:]...), append(v[:3], scratch[:]...)
		llrb.Set(key, val, nil)
	}
	return llrb
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

func testgetnext(t *testing.T, cur api.Cursor, from int, keys, vals []string) {
	i := from
	for {
		k, deleted := cur.Key()
		if string(k) != keys[i] {
			t.Errorf("for %v expected %s, got %s", i, keys[i], k)
		} else if string(k) == keys[15] && deleted == false {
			t.Errorf("expected key deleted")
		} else if string(k) != keys[15] && deleted == true {
			t.Errorf("unexpected deleted")
		}
		v := cur.Value()
		if string(v) != vals[i] {
			t.Errorf("for %v expected %s, got %s", i, vals[i], v)
		}
		k, v, deleted, _ = cur.GetNext()
		i++
		if k == nil {
			break
		} else if string(k) == keys[15] && deleted == false {
			t.Errorf("expected key deleted")
		} else if string(k) != keys[15] && deleted == true {
			t.Errorf("%s unexpected deleted", k)
		} else if string(k) != keys[i] {
			t.Errorf("for %v expected %s, got %s", i, keys[i], k)
		} else if string(v) != vals[i] {
			t.Errorf("for %v expected %s, got %s", i, vals[i], v)
		}
	}
	if i != len(keys) {
		t.Errorf("iterated till %v", i)
	}
}

func testynext(t *testing.T, cur api.Cursor, from int, keys, vals []string) {
	i := from
	for {
		k, v, _, deleted, _ := cur.YNext(false /*fin*/)
		if k == nil {
			break
		} else if string(k) == keys[15] && deleted == false {
			t.Errorf("expected key deleted")
		} else if string(k) != keys[15] && deleted == true {
			t.Errorf("unexpected deleted")
		} else if string(k) != keys[i] {
			t.Errorf("for %v expected %s, got %s", i, keys[i], k)
		} else if string(v) != vals[i] {
			t.Errorf("for %v expected %s, got %s", i, vals[i], v)
		}
		i++
	}
	if i != len(keys) {
		t.Errorf("iterated till %v", i)
	}
}

func makeLLRB(n int) (*LLRB, [][]byte) {
	mi := NewLLRB("buildllrb", Defaultsettings())
	k, v := []byte("key000000000000"), []byte("val00000000000000")
	keys := [][]byte{}
	for i := 0; i < n; i++ {
		x := fmt.Sprintf("%d", i)
		key, val := append(k[:3], x...), append(v[:3], x...)
		mi.Set(key, val, nil)
		if i%10 == 0 {
			mi.Delete(key, nil, true /*lsm*/)
		}
		keys = append(keys, key)
	}
	return mi, keys
}

//buf := bytes.NewBuffer(nil)
//llrb.Dotdump(buf)
//ioutil.WriteFile("out.dot", buf.Bytes(), 0664)
