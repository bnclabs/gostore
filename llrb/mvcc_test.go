package llrb

import "io"
import "fmt"
import "time"
import "bytes"
import "testing"
import "io/ioutil"
import "encoding/binary"

import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/api"
import s "github.com/prataprc/gosettings"

func TestMVCCEmpty(t *testing.T) {
	setts := s.Settings{"memcapacity": 10 * 1024 * 1024}
	mvcc := NewMVCC("empty", setts)
	defer mvcc.Destroy()

	if mvcc.ID() != "empty" {
		t.Errorf("unexpected %v", mvcc.ID())
	}

	if mvcc.Count() != 0 {
		t.Errorf("unexpected %v", mvcc.Count())
	}

	// validate statistics
	mvcc.Validate()
	stats := mvcc.Stats()
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

	mvcc.Log()
}

func TestMVCCLoad(t *testing.T) {
	var cas uint64

	setts := s.Settings{"memcapacity": 10 * 1024 * 1024}
	mvcc := NewMVCC("load", setts)
	defer mvcc.Destroy()

	if mvcc.ID() != "load" {
		t.Errorf("unexpected %v", mvcc.ID())
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
		oldvalue, cas = mvcc.Set(k, v, oldvalue)
		if len(oldvalue) > 0 {
			t.Errorf("unexpected old value %s", oldvalue)
		} else if cas != uint64(i+1) {
			t.Errorf("expected %v, got %v, key %s", i+1, cas, key)
		}
		mvcc.Validate()
	}

	// test loaded data
	value := make([]byte, 1024)
	for i, key := range keys {
		if value, cas, _, ok := mvcc.Get(lib.Str2bytes(key), value); !ok {
			t.Errorf("expected key %s", key)
		} else if string(value) != vals[i] {
			t.Errorf("expected %s, got %s, key %s", vals[i], value, key)
		} else if cas != uint64(i)+1 {
			t.Errorf("expected %v, got %v, key %s", i+1, cas, key)
		}
	}
	// test set.
	k, v := []byte(keys[0]), []byte("newvalue")
	oldvalue, cas = mvcc.Set(k, v, oldvalue)
	if cas != uint64(len(keys)+1) {
		t.Errorf("expected %v, got %v, key %s", len(keys)+1, cas, k)
	} else if string(oldvalue) != vals[0] {
		t.Errorf("expected %s, got %s", vals[0], oldvalue)
	}
	mvcc.Validate()
	// test set with nil for oldvalue.
	nilvalue := []byte(nil)
	k, v = []byte(keys[0]), []byte("newvalue1")
	nilvalue, cas = mvcc.Set(k, v, nil)
	if cas != uint64(len(keys)+2) {
		t.Errorf("expected %v, got %v, key %s", len(keys)+1, cas, k)
	} else if len(nilvalue) != 0 {
		t.Errorf("unexpected %s", nilvalue)
	}
	mvcc.Validate()
	// test set with value nil.
	k, v = []byte(keys[0]), nil
	oldvalue, cas = mvcc.Set(k, v, oldvalue)
	if cas != uint64(len(keys)+3) {
		t.Errorf("expected %v, got %v, key %s", len(keys)+1, cas, k)
	} else if string(oldvalue) != "newvalue1" {
		t.Errorf("unexpected %q", oldvalue)
	}
	mvcc.Validate()
	// test set with oldvalue nil.
	k, v = []byte(keys[0]), []byte("newvalue2")
	oldvalue, cas = mvcc.Set(k, v, nil)
	if cas != uint64(len(keys)+4) {
		t.Errorf("expected %v, got %v, key %s", len(keys)+1, cas, k)
	} else if len(oldvalue) != 0 {
		t.Errorf("unexpected %s", oldvalue)
	}
	mvcc.Validate()
	if value, cas, deleted, ok := mvcc.Get(k, value); ok == false {
		t.Errorf("unexpected false")
	} else if deleted == true {
		t.Errorf("expected key")
	} else if cas != uint64(len(keys)+4) {
		t.Errorf("unexpected %v", cas)
	} else if string(value) != "newvalue2" {
		t.Errorf("unexpected value %s", value)
	}

	if mvcc.Count() != int64(len(keys)) {
		t.Errorf("unexpected %v", mvcc.Count())
	}

	// validate
	stats := mvcc.Stats()
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
	} else if x := stats["n_clones"].(int64); x != 83 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_nodes"].(int64); x != int64(len(keys)) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_txns"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if u := nodeutz(stats); u < 10.0 {
		t.Errorf("unexpected %v", u)
	} else if u := valueutz(stats); u < 10.0 {
		t.Errorf("unexpected %v", u)
	}
}

func TestMVCCDotdump(t *testing.T) {
	setts := s.Settings{"memcapacity": 10 * 1024 * 1024}
	mvcc := NewMVCC("load", setts)
	defer mvcc.Destroy()

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
		mvcc.Set(k, v, oldvalue)
	}

	buf := bytes.NewBuffer(nil)
	mvcc.Dotdump(buf)
	data, err := ioutil.ReadFile("testdata/mvccload.dot")
	if err != nil {
		t.Error(err)
	}
	if out := append(buf.Bytes()); bytes.Compare(data, out) != 0 {
		t.Errorf("mismatch in dotdump")
		t.Errorf("%s", out)
		t.Errorf("%s", data)
	}
}

func TestMVCCLoadLarge(t *testing.T) {
	setts := s.Settings{"memcapacity": 100 * 1024 * 1024}
	mvcc := NewMVCC("loadlarge", setts)
	defer mvcc.Destroy()

	// load data
	n, oldvalue, rkm, rvm := 1000, make([]byte, 1024), 0, 0
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%v", i))
		v := []byte(fmt.Sprintf("val%v", i))
		oldvalue, _ /*cas*/ = mvcc.Set(k, v, oldvalue)
		rkm, rvm = rkm+len(k), rvm+len(v)
		mvcc.Validate()
	}
	// test loaded data
	value := make([]byte, 1024)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		if value, cas, _, ok := mvcc.Get(lib.Str2bytes(key), value); !ok {
			t.Errorf("expected key %s", key)
		} else if string(value) != val {
			t.Errorf("expected %s, got %s, key %s", val, value, key)
		} else if cas != uint64(i)+1 {
			t.Errorf("expected %v, got %v, key %s", i+1, cas, key)
		}
	}

	if mvcc.Count() != int64(n) {
		t.Errorf("unexpected %v", mvcc.Count())
	}

	// validate
	stats := mvcc.Stats()
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
	} else if x := stats["n_clones"].(int64); x != 11380 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_nodes"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_txns"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if u := nodeutz(stats); u < 40 {
		t.Errorf("unexpected %v", u)
	} else if u := valueutz(stats); u < 40 {
		t.Errorf("unexpected %v", u)
	}
}

func TestMVCCClone(t *testing.T) {
	setts := s.Settings{"memcapacity": 100 * 1024 * 1024}
	mvcc := NewMVCC("clone", setts)
	defer mvcc.Destroy()

	// load data
	n, oldvalue, rkm, rvm := 1000, make([]byte, 1024), 0, 0
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%v", i))
		v := []byte(fmt.Sprintf("val%v", i))
		oldvalue, _ /*cas*/ = mvcc.Set(k, v, oldvalue)
		rkm, rvm = rkm+len(k), rvm+len(v)
		mvcc.Validate()
	}
	clone := mvcc.Clone("loadclone")
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
		mvcc.Validate()
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
	} else if x := stats["n_clones"].(int64); x != 11380 {
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

func TestMVCCSetCAS(t *testing.T) {
	var err error
	var cas uint64

	setts := s.Settings{"memcapacity": 100 * 1024 * 1024}
	mvcc := NewMVCC("setcas", setts)
	defer mvcc.Destroy()

	// load data
	n, oldvalue, rkm, rvm := 1000, make([]byte, 1024), 0, 0
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%v", i))
		v := []byte(fmt.Sprintf("val%v", i))
		if oldvalue, cas, err = mvcc.SetCAS(k, v, oldvalue, 0); err != nil {
			t.Error(err)
		} else if len(oldvalue) > 0 {
			t.Errorf("unexpected oldvalue %s", oldvalue)
		} else if cas != uint64(i+1) {
			t.Errorf("expected %v, got %v, key %s", i+1, cas, k)
		}
		rkm, rvm = rkm+len(k), rvm+len(v)
		mvcc.Validate()
	}

	testwithkey := func(k []byte, v []byte, byt byte, xcas uint64) {
		var ncas, dcas uint64

		refv := make([]byte, 1024)
		refv = refv[:copy(refv, v)]
		// set with cas
		v = append(v, byt)
		oldvalue, ncas, err = mvcc.SetCAS(k, v, oldvalue, xcas)
		if err != nil {
			t.Error(err)
		} else if string(oldvalue) != string(refv) {
			t.Errorf("unexpected %s, %s", oldvalue, refv)
		} else if ncas != cas+1 {
			t.Errorf("expected %v, got %v", cas+1, ncas)
		}
		cas, rvm = ncas, rvm-len(oldvalue)+len(v)
		mvcc.Validate()
		refv = refv[:copy(refv[:1024], v)]
		// set with invalid cas
		oldvalue, xcas, err = mvcc.SetCAS(k, nil, oldvalue, 1000000000)
		if err.Error() != api.ErrorInvalidCAS.Error() {
			t.Errorf("expected error")
		}
		mvcc.Validate()
		// delete key with lsm
		oldvalue, ncas = mvcc.Delete(k, oldvalue, true /*lsm*/)
		if string(oldvalue) != string(refv) {
			t.Errorf("unexpected %s, %s", oldvalue, refv)
		} else if ncas != cas+1 {
			t.Errorf("expected %v, got %v", cas+1, ncas)
		}
		cas, dcas, rvm = 0, ncas, rvm-len(oldvalue)
		mvcc.Validate()
		// set with mismatch cas for deleted key.
		oldvalue, xcas, err = mvcc.SetCAS(k, v, oldvalue, 1000000000000)
		if err.Error() != api.ErrorInvalidCAS.Error() {
			t.Errorf("expected error")
		}
		mvcc.Validate()
		// set with mismatch cas and oldvalue as nil.
		nilvalue := []byte(nil)
		nilvalue, xcas, err = mvcc.SetCAS(k, v, nil, 100000000000000000)
		if err.Error() != api.ErrorInvalidCAS.Error() {
			t.Errorf("expected error")
		} else if nilvalue != nil {
			t.Errorf("unexpected %s", nilvalue)
		}
		mvcc.Validate()
		// set with cas and value nil.
		oldvalue, ncas, err = mvcc.SetCAS(k, nil, oldvalue, cas)
		if err != nil {
			t.Error(err)
		} else if ncas != dcas+1 {
			t.Errorf("unexpected %v", ncas)
		} else if string(oldvalue) != string(refv) {
			t.Fatalf("unexpected %s %q", oldvalue, refv)
		}
		cas = ncas
		mvcc.Validate()
		// set with cas and oldvalue nil.
		v = append(v, byt)
		nilvalue, ncas, err = mvcc.SetCAS(k, v, nil, cas)
		if err != nil {
			t.Error(err)
		} else if ncas != cas+1 {
			t.Errorf("unexpected %v", ncas)
		} else if nilvalue != nil {
			t.Errorf("unexpected %s", nilvalue)
		}
		cas, rvm = ncas, rvm+len(v)
		mvcc.Validate()
		// set missing with cas
		k = []byte("missing")
		oldvalue, xcas, err = mvcc.SetCAS(k, v, oldvalue, cas)
		if err.Error() != api.ErrorInvalidCAS.Error() {
			t.Errorf("unexpected %v", err)
		}
		mvcc.Validate()
	}

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%v", i))
		val := []byte(fmt.Sprintf("val%v", i))
		testwithkey(key, val, byte(48+(i%10)), uint64(i+1))
	}

	// test loaded data
	value := make([]byte, 1024)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%v", i))
		refv := []byte(fmt.Sprintf("val%v", i))
		byt := byte(48 + (i % 10))
		refv = append(refv, byt, byt)
		if value, _, _, ok := mvcc.Get(key, value); !ok {
			t.Errorf("expected key %s", key)
		} else if string(value) != string(refv) {
			t.Errorf("expected %s, got %s, key %s", refv, value, key)
		} else {
			if string(value) != string(refv) {
				t.Errorf("expected %s, got %s, key %s", refv, value, key)
			}
		}
		mvcc.Validate()
	}

	if mvcc.Count() != int64(n) {
		t.Errorf("unexpected %v", mvcc.Count())
	}

	// validate
	stats := mvcc.Stats()
	if x := stats["keymemory"].(int64); x != int64(rkm) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["valmemory"].(int64); x != int64(rvm) {
		t.Errorf("unexpected %v, %v", x, rvm)
	} else if x := stats["n_count"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_inserts"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_updates"].(int64); x != 3000 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_deletes"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_clones"].(int64); x != 38626 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_nodes"].(int64); x != int64(n) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_txns"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if u := nodeutz(stats); u < 40 {
		t.Errorf("unexpected %v", u)
	} else if u := valueutz(stats); u < 40 {
		t.Errorf("unexpected %v", u)
	}
}

func TestMVCCDelete(t *testing.T) {
	var err error
	var cas uint64

	setts := s.Settings{"memcapacity": 100 * 1024 * 1024}
	mvcc := NewMVCC("delete", setts)
	defer mvcc.Destroy()

	// load data
	n, oldvalue, rkm, rvm := 1000, make([]byte, 1024), 0, 0
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%v", i))
		v := []byte(fmt.Sprintf("val%v", i))
		if oldvalue, cas, err = mvcc.SetCAS(k, v, oldvalue, 0); err != nil {
			t.Error(err)
		} else if len(oldvalue) > 0 {
			t.Errorf("unexpected oldvalue %s", oldvalue)
		} else if cas != uint64(i+1) {
			t.Errorf("expected %v, got %v, key %s", i+1, cas, k)
		}
		rkm, rvm = rkm+len(k), rvm+len(v)
		mvcc.Validate()
	}
	// delete missing key
	k := []byte("missing")
	oldvalue, cas = mvcc.Delete(k, oldvalue, false /*lsm*/)
	if cas != uint64(n+1) {
		t.Errorf("expected %v, got %v", n+1, cas)
	} else if len(oldvalue) > 0 {
		t.Errorf("unexpected %s", oldvalue)
	}
	mvcc.Validate()
	// mutation: delete a valid key
	k, v := []byte("key100"), []byte("val100")
	oldvalue, cas = mvcc.Delete(k, oldvalue, false /*lsm*/)
	if cas != uint64(n+2) {
		t.Errorf("expected %v, got %v", n+2, cas)
	} else if string(oldvalue) != "val100" {
		t.Errorf("unexpected %s", oldvalue)
	}
	rkm, rvm = rkm-len(k), rvm-len(v)
	mvcc.Validate()
	// test with get
	if oldvalue, cas, delok, ok := mvcc.Get(k, oldvalue); ok {
		t.Errorf("unexpected key %s", k)
	} else if delok == true {
		t.Errorf("unexpected true")
	} else if cas != 0 {
		t.Errorf("unexpected cas %v", cas)
	} else if len(oldvalue) > 0 {
		t.Errorf("unexpected %s", oldvalue)
	}
	mvcc.Validate()
	// mutation: set-cas on deleted key
	k, v = []byte("key100"), []byte("valu100")
	oldvalue, cas, err = mvcc.SetCAS(k, v, oldvalue, 0)
	if err != nil {
		t.Error(err)
	} else if cas != uint64(n+3) {
		t.Errorf("expected %v, got %v", n+3, cas)
	}
	rkm, rvm = rkm+len(k), rvm+len(v)
	mvcc.Validate()
	// mutation: delete with lsm
	k = []byte("key100")
	oldvalue, cas = mvcc.Delete(k, oldvalue, true /*lsm*/)
	if cas != uint64(n+4) {
		t.Errorf("expected %v, got %v", n+4, cas)
	} else if string(oldvalue) != "valu100" {
		t.Errorf("unexpected %s", oldvalue)
	}
	rkm, rvm = rkm-len(k), rvm-len(v)
	mvcc.Validate()
	// test with get lsm deleted key
	if oldvalue, cas, delok, ok := mvcc.Get(k, oldvalue); !ok {
		t.Errorf("expected key %s", k)
	} else if delok == false {
		t.Errorf("expected true")
	} else if cas != uint64(n+4) {
		t.Errorf("expected %v, got %v", n+4, cas)
	} else if string(oldvalue) != "valu100" {
		t.Errorf("unexpected %s", oldvalue)
	}
	mvcc.Validate()
	// mutation: set-cas on deleted key
	k, v = []byte("key100"), []byte("value100")
	oldvalue, cas, err = mvcc.SetCAS(k, v, oldvalue, 0)
	if err != nil {
		t.Error(err)
	} else if cas != uint64(n+5) {
		t.Errorf("expected %v, got %v", n+5, cas)
	} else if string(oldvalue) > "valu100" {
		t.Errorf("unexpected %s", oldvalue)
	}
	rkm, rvm = rkm+len(k), rvm+len(v)
	mvcc.Validate()
	// test with get
	if oldvalue, cas, delok, ok := mvcc.Get(k, oldvalue); !ok {
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
	oldvalue, cas = mvcc.Delete(k, oldvalue, true /*lsm*/)
	if cas != uint64(n+6) {
		t.Errorf("expected %v, got %v", n+6, cas)
	} else if len(oldvalue) > 0 {
		t.Errorf("unexpected %s", oldvalue)
	}
	rkm = rkm + len(k)
	mvcc.Validate()
	// test with get
	if oldvalue, cas, delok, ok := mvcc.Get(k, oldvalue); !ok {
		t.Errorf("expected key %s", k)
	} else if delok == false {
		t.Errorf("unexpected false")
	} else if cas != uint64(n+6) {
		t.Errorf("expected %v, got %v", n+6, cas)
	} else if len(oldvalue) > 0 {
		t.Errorf("unexpected %s", oldvalue)
	}
	mvcc.Validate()

	// test loaded data
	value := make([]byte, 1024)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		if value, cas, _, ok := mvcc.Get(lib.Str2bytes(key), value); !ok {
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
	}
	mvcc.Validate()

	if mvcc.Count() != int64(n+1) {
		t.Errorf("unexpected %v", mvcc.Count())
	}

	// validate
	stats := mvcc.Stats()
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
	} else if x := stats["n_clones"].(int64); x != 11509 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_nodes"].(int64); x != int64(n+2) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_txns"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if u := nodeutz(stats); u < 45 {
		t.Errorf("unexpected %v", u)
	} else if u := valueutz(stats); u < 45 {
		t.Errorf("unexpected %v", u)
	}

	// delete all keys
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%v", i))
		oldvalue, cas = mvcc.Delete(k, oldvalue, false)
		if cas != uint64(n+6+i+1) {
			t.Errorf("expected %v, got %v, key %s", n+6+i+1, cas, k)
		}
		mvcc.Validate()
	}
	k = []byte("missing")
	mvcc.Delete(k, oldvalue, false /*lsm*/)

	if mvcc.Count() != 0 {
		t.Errorf("unexpected %v", mvcc.Count())
	}

	// validate
	stats = mvcc.Stats()
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
	} else if x := stats["n_clones"].(int64); x != 38491 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_nodes"].(int64); x != int64(n+2) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_txns"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	}
}

func TestMVCCTxn(t *testing.T) {
	setts := s.Settings{"memcapacity": 10 * 1024 * 1024}
	mvcc := NewMVCC("txn", setts)
	defer mvcc.Destroy()
	snaptick := time.Duration(Defaultsettings().Int64("snapshottick") * 2)
	snaptick = snaptick * time.Millisecond

	// First transaction
	txn := mvcc.BeginTxn(0x1234)
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
	oldvalue, deleted, ok := txn.Get(key, oldvalue)
	if ok == false {
		t.Errorf("unexpected false")
	} else if deleted == true {
		t.Errorf("unexpected deleted")
	} else if string(oldvalue) != "value1" {
		t.Errorf("unexpected %s", oldvalue)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	// verify first transaction
	key, value = []byte("buckeroo"), []byte{}
	value, _, deleted, ok = mvcc.Get(key, value)
	if ok == false {
		t.Errorf("unexpected false")
	} else if deleted == true {
		t.Errorf("unexpected delete")
	} else if string(value) != "value2" {
		t.Errorf("unexpected %s", value)
	}

	time.Sleep(snaptick)

	// Second transaction
	txn = mvcc.BeginTxn(0x12345)
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
	value, deleted, ok = txn.Get(key, []byte{})
	if deleted == false {
		t.Errorf("expected as deleted")
	} else if ok == false {
		t.Errorf("expected key")
	} else if len(value) > 0 {
		t.Errorf("unexpected %s", value)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	// verify second transaction
	key, value = []byte("plumless"), []byte{}
	value, _, deleted, ok = mvcc.Get(key, value)
	if ok == false {
		t.Errorf("unexpected false")
	} else if deleted == false {
		t.Errorf("expected delete")
	} else if string(value) != "value1" {
		t.Errorf("unexpected %s", value)
	}

	time.Sleep(snaptick)

	// third transaction abort
	txn = mvcc.BeginTxn(0)
	// set initial values
	key, value = []byte("plumless"), []byte("aborted")
	txn.Set(key, value, nil)
	txn.Abort()
	// get entries
	key = []byte("plumless")
	value, _, _ = txn.Get(key, value)
	if string(value) != "value1" {
		t.Errorf("unexpected %s", value)
	}
}

func TestMVCCView(t *testing.T) {
	setts := s.Settings{"memcapacity": 10 * 1024 * 1024}
	mvcc := NewMVCC("view", setts)
	defer mvcc.Destroy()
	snaptick := time.Duration(Defaultsettings().Int64("snapshottick") * 2)
	snaptick = snaptick * time.Millisecond

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
		mvcc.Set(k, v, nil)
	}

	time.Sleep(snaptick)

	view := mvcc.View(0x1234)
	defer view.Abort()

	if view.ID() != 0x1234 {
		t.Errorf("unexpected %v", view.ID())
	}
	var deleted, ok bool
	value := []byte{}
	for i, key := range keys {
		k := lib.Str2bytes(key)
		value, deleted, ok = view.Get(k, value)
		if string(value) != vals[i] {
			t.Errorf("for %v expected %v, got %v", i, vals[i], value)
		} else if deleted == true {
			t.Errorf("unexpected deleted")
		} else if ok == false {
			t.Errorf("key %s missing", k)
		}
	}
}

func TestMVCCTxnCursor(t *testing.T) {
	setts := s.Settings{"memcapacity": 10 * 1024 * 1024}
	mvcc := NewMVCC("view", setts)
	defer mvcc.Destroy()

	snaptick := time.Duration(Defaultsettings().Int64("snapshottick") * 2)
	snaptick = snaptick * time.Millisecond

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
		mvcc.Set(k, v, nil)
	}
	mvcc.Delete([]byte(keys[15]), nil, true /*lsm*/)

	// mvcc.getroot().ptrdump(" ")

	time.Sleep(snaptick)

	txn := mvcc.BeginTxn(0x1234)
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
	cur, _ := txn.OpenCursor([]byte(keys[0]))
	cur.Delcursor(true /*lsm*/)
	cur.Delete([]byte(keys[1]), nil, true /*lsm*/)
	value := []byte("newvalue")
	cur.Set([]byte(keys[2]), value, nil)
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	value, _, deleted, ok := mvcc.Get([]byte(keys[0]), []byte{})
	if deleted == false {
		t.Errorf("expected deleted")
	} else if ok == false {
		t.Errorf("expected key")
	} else if string(value) != vals[0] {
		t.Errorf("expected %s, got %s", vals[0], value)
	}
	value, _, deleted, ok = mvcc.Get([]byte(keys[1]), []byte{})
	if deleted == false {
		t.Errorf("expected deleted")
	} else if ok == false {
		t.Errorf("expected key")
	} else if string(value) != vals[1] {
		t.Errorf("expected %s, got %s", vals[1], value)
	}
	value, _, deleted, ok = mvcc.Get([]byte(keys[2]), []byte{})
	if deleted == true {
		t.Errorf("unexpected deleted")
	} else if ok == false {
		t.Errorf("expected key")
	} else if string(value) != "newvalue" {
		t.Errorf("unexpected %s", value)
	}
}

func TestMVCCViewCursor(t *testing.T) {
	setts := s.Settings{"memcapacity": 10 * 1024 * 1024}
	mvcc := NewMVCC("view", setts)
	defer mvcc.Destroy()

	snaptick := time.Duration(Defaultsettings().Int64("snapshottick") * 2)
	snaptick = snaptick * time.Millisecond

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
		mvcc.Set(k, v, nil)
	}
	mvcc.Delete([]byte(keys[15]), nil, true /*lsm*/)

	// mvcc.getroot().ptrdump(" ")
	time.Sleep(snaptick)

	for i, key := range keys {
		view := mvcc.View(0x1234 + uint64(i))
		cur, _ := view.OpenCursor([]byte(key))
		testgetnext(t, cur, i, keys, vals)
		view.Abort()
		view = mvcc.View(0)
		cur, _ = view.OpenCursor([]byte(key))
		testynext(t, cur, i, keys, vals)
		view.Abort()
	}

	time.Sleep(snaptick)

	txn := mvcc.BeginTxn(0x12345)
	cur, _ := txn.OpenCursor([]byte(keys[0]))
	cur.Delcursor(true /*lsm*/)
	cur.Delete([]byte(keys[1]), nil, true /*lsm*/)
	value := []byte("newvalue")
	cur.Set([]byte(keys[2]), value, nil)
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	value, _, deleted, ok := mvcc.Get([]byte(keys[0]), []byte{})
	if deleted == false {
		t.Errorf("expected deleted")
	} else if ok == false {
		t.Errorf("expected key")
	} else if string(value) != vals[0] {
		t.Errorf("expected %s, got %s", vals[0], value)
	}
	value, _, deleted, ok = mvcc.Get([]byte(keys[1]), []byte{})
	if deleted == false {
		t.Errorf("expected deleted")
	} else if ok == false {
		t.Errorf("expected key")
	} else if string(value) != vals[1] {
		t.Errorf("expected %s, got %s", vals[1], value)
	}
	value, _, deleted, ok = mvcc.Get([]byte(keys[2]), []byte{})
	if deleted == true {
		t.Errorf("unexpected deleted")
	} else if ok == false {
		t.Errorf("expected key")
	} else if string(value) != "newvalue" {
		t.Errorf("unexpected %s", value)
	}
}

func TestMVCCScan(t *testing.T) {
	setts := s.Settings{"memcapacity": 100 * 1024 * 1024}
	llrb := NewMVCC("scan", setts)
	defer llrb.Destroy()
	snaptick := time.Duration(Defaultsettings().Int64("snapshottick") * 2)
	snaptick = snaptick * time.Millisecond

	// load data
	scanlimit = 2
	n := 10000
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%v", i))
		v := []byte(fmt.Sprintf("val%v", i))
		llrb.Set(k, v, nil)
		llrb.Validate()
	}

	time.Sleep(snaptick)

	view := llrb.View(0)
	defer view.Abort()
	count := 0
	cur, _ := view.OpenCursor(nil)
	scan := llrb.Scan()

	refkey, refval, refseqno, refdeleted, _ := cur.YNext(false /*fin*/)
	key, val, seqno, deleted, err := scan(false /*close*/)
	if err != nil {
		t.Fatal(err)
	}
	for refkey != nil {
		count++
		//t.Logf("iter %q scan:%q", refkey, key)
		if bytes.Compare(key, refkey) != 0 {
			t.Errorf("expected %q, got %q", refkey, key)
		} else if bytes.Compare(val, refval) != 0 {
			t.Errorf("expected %s, got %s", refval, val)
		} else if seqno != refseqno {
			t.Errorf("expected %v, got %v", refseqno, seqno)
		} else if deleted != refdeleted {
			t.Errorf("expected %v, got %v", refdeleted, deleted)
		}
		refkey, refval, refseqno, refdeleted, _ = cur.YNext(false /*fin*/)
		key, val, seqno, deleted, err = scan(false /*close*/)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
	}
	if key != nil {
		t.Errorf("expected nil, %s", key)
	} else if count != n {
		t.Errorf("expected %v, got %v", n, count)
	}
}

func BenchmarkMVCCCount(b *testing.B) {
	mvcc := makeBenchMVCC(1000)
	defer mvcc.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mvcc.Count()
	}
}

func BenchmarkMVCCSet(b *testing.B) {
	var scratch [8]byte

	setts := s.Settings{"memcapacity": 10 * 1024 * 1024}
	mvcc := NewMVCC("bench", setts)
	defer mvcc.Destroy()

	b.ResetTimer()
	k, v := []byte("key000000000000"), []byte("val00000000000000")
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(scratch[:], uint64(i+1))
		key, val := append(k[:3], scratch[:]...), append(v[:3], scratch[:]...)
		mvcc.Set(key, val, nil)
	}
}

func BenchmarkMVCCCAS(b *testing.B) {
	var scratch [8]byte

	setts := s.Settings{"memcapacity": 10 * 1024 * 1024}
	mvcc := NewMVCC("bench", setts)
	defer mvcc.Destroy()

	b.ResetTimer()
	k, v := []byte("key000000000000"), []byte("val00000000000000")
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(scratch[:], uint64(i+1))
		key, val := append(k[:3], scratch[:]...), append(v[:3], scratch[:]...)
		mvcc.SetCAS(key, val, nil, 0)
	}
}

func BenchmarkMVCCDel(b *testing.B) {
	var scratch [8]byte

	mvcc := makeBenchMVCC(b.N)
	defer mvcc.Destroy()

	b.ResetTimer()
	k := []byte("key000000000000")
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(scratch[:], uint64(i+1))
		key := append(k[:3], scratch[:]...)
		mvcc.Delete(key, nil, false /*lsm*/)
	}
}

func BenchmarkMVCCDelLSM(b *testing.B) {
	var scratch [8]byte

	mvcc := makeBenchMVCC(b.N)
	defer mvcc.Destroy()

	b.ResetTimer()
	k := []byte("key000000000000")
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(scratch[:], uint64(i+1))
		key := append(k[:3], scratch[:]...)
		mvcc.Delete(key, nil, true /*lsm*/)
	}
}

func BenchmarkMVCCGet(b *testing.B) {
	var scratch [8]byte

	mvcc := makeBenchMVCC(b.N)
	defer mvcc.Destroy()

	b.ResetTimer()
	k := []byte("key000000000000")
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(scratch[:], uint64(i+1))
		key := append(k[:3], scratch[:]...)
		mvcc.Get(key, nil)
	}
}

func BenchmarkMVCCTxn(b *testing.B) {
	mvcc := makeBenchMVCC(1000)
	defer mvcc.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := mvcc.BeginTxn(0)
		txn.Commit()
	}
}

func BenchmarkMVCCClone(b *testing.B) {
	mvcc := makeBenchMVCC(b.N)
	defer mvcc.Destroy()

	b.ResetTimer()
	mvcc.Clone("benchclone")
}

func BenchmarkMVCCView(b *testing.B) {
	mvcc := makeBenchMVCC(1000)
	defer mvcc.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := mvcc.View(0)
		txn.Abort()
	}
}

func BenchmarkMVCCOpenCur(b *testing.B) {
	mvcc := makeBenchMVCC(1000)
	defer mvcc.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := mvcc.BeginTxn(0)
		txn.OpenCursor([]byte("key500"))
		txn.Commit()
	}
}

func BenchmarkMVCCGetNext(b *testing.B) {
	mvcc := makeBenchMVCC(b.N)
	defer mvcc.Destroy()

	txn := mvcc.BeginTxn(0)
	cur, _ := txn.OpenCursor(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur.GetNext()
	}
	txn.Commit()
}

func BenchmarkMVCCYNext(b *testing.B) {
	mvcc := makeBenchMVCC(b.N)
	defer mvcc.Destroy()

	view := mvcc.BeginTxn(0)
	cur, _ := view.OpenCursor(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur.YNext(false /*fin*/)
	}
	view.Abort()
}

func BenchmarkMVCCScan(b *testing.B) {
	llrb := makeBenchLLRB(b.N)
	defer llrb.Destroy()

	scan := llrb.Scan()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan(false /*close*/)
	}
}

func makeBenchMVCC(n int) *MVCC {
	var scratch [8]byte

	setts := s.Settings{"memcapacity": 10 * 1024 * 1024}
	mvcc := NewMVCC("bench", setts)
	k, v := []byte("key000000000000"), []byte("val00000000000000")
	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(scratch[:], uint64(i+1))
		key, val := append(k[:3], scratch[:]...), append(v[:3], scratch[:]...)
		mvcc.Set(key, val, nil)
	}
	return mvcc
}

//buf := bytes.NewBuffer(nil)
//mvcc.Dotdump(buf)
//ioutil.WriteFile("out.dot", buf.Bytes(), 0664)
