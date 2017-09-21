package llrb

import "fmt"
import "bytes"
import "time"
import "testing"
import "io/ioutil"
import "encoding/binary"

import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/api"
import s "github.com/prataprc/gosettings"

func TestMVCCEmpty(t *testing.T) {
	setts := s.Settings{
		"keycapacity": 10 * 1024 * 1024, "valcapacity": 10 * 1024 * 1024,
	}
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

	setts := s.Settings{
		"keycapacity": 10 * 1024 * 1024, "valcapacity": 10 * 1024 * 1024,
	}
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
	}
	time.Sleep(10 * time.Millisecond)
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
	//// test set.
	//k, v := []byte(keys[0]), []byte("newvalue")
	//oldvalue, cas = mvcc.Set(k, v, oldvalue)
	//if cas != uint64(len(keys)+1) {
	//	t.Errorf("expected %v, got %v, key %s", len(keys)+1, cas, k)
	//} else if string(oldvalue) != vals[0] {
	//	t.Errorf("expected %s, got %s", vals[0], oldvalue)
	//}
	//// test set with nil for oldvalue.
	//nilvalue := []byte(nil)
	//k, v = []byte(keys[0]), []byte("newvalue1")
	//nilvalue, cas = mvcc.Set(k, v, nil)
	//if cas != uint64(len(keys)+2) {
	//	t.Errorf("expected %v, got %v, key %s", len(keys)+1, cas, k)
	//} else if len(nilvalue) != 0 {
	//	t.Errorf("unexpected %s", nilvalue)
	//}
	//// test set with value nil.
	//k, v = []byte(keys[0]), nil
	//oldvalue, cas = mvcc.Set(k, v, oldvalue)
	//if cas != uint64(len(keys)+3) {
	//	t.Errorf("expected %v, got %v, key %s", len(keys)+1, cas, k)
	//} else if string(oldvalue) != "newvalue1" {
	//	t.Errorf("unexpected %q", oldvalue)
	//}
	//// test set with oldvalue nil.
	//k, v = []byte(keys[0]), []byte("newvalue2")
	//oldvalue, cas = mvcc.Set(k, v, nil)
	//if cas != uint64(len(keys)+4) {
	//	t.Errorf("expected %v, got %v, key %s", len(keys)+1, cas, k)
	//} else if len(oldvalue) != 0 {
	//	t.Errorf("unexpected %s", oldvalue)
	//}
	//if value, cas, deleted, ok := mvcc.Get(k, value); ok == false {
	//	t.Errorf("unexpected false")
	//} else if deleted == true {
	//	t.Errorf("expected key")
	//} else if cas != uint64(len(keys)+4) {
	//	t.Errorf("unexpected %v", cas)
	//} else if string(value) != "newvalue2" {
	//	t.Errorf("unexpected value %s", value)
	//}

	//if mvcc.Count() != int64(len(keys)) {
	//	t.Errorf("unexpected %v", mvcc.Count())
	//}

	//// validate
	//mvcc.Validate()
	//stats := mvcc.Stats()
	//if x := stats["keymemory"].(int64); x != 72 {
	//	t.Errorf("unexpected %v", x)
	//} else if x := stats["valmemory"].(int64); x != 77 {
	//	t.Errorf("unexpected %v", x)
	//} else if x := stats["n_count"].(int64); x != int64(len(keys)) {
	//	t.Errorf("unexpected %v", x)
	//} else if x := stats["n_inserts"].(int64); x != int64(len(keys)) {
	//	t.Errorf("unexpected %v", x)
	//} else if x := stats["n_updates"].(int64); x != 4 {
	//	t.Errorf("unexpected %v", x)
	//} else if x := stats["n_deletes"].(int64); x != 0 {
	//	t.Errorf("unexpected %v", x)
	//} else if x := stats["n_clones"].(int64); x != 4 {
	//	t.Errorf("unexpected %v", x)
	//} else if x := stats["n_frees"].(int64); x != 4 {
	//	t.Errorf("unexpected %v", x)
	//} else if x := stats["n_nodes"].(int64); x != int64(len(keys)) {
	//	t.Errorf("unexpected %v", x)
	//} else if x := stats["n_txns"].(int64); x != 0 {
	//	t.Errorf("unexpected %v", x)
	//} else if u := nodeutz(stats); u < 50.0 {
	//	t.Errorf("unexpected %v", u)
	//} else if u := valueutz(stats); u < 50.0 {
	//	t.Errorf("unexpected %v", u)
	//}
}

func TestMVCCDotdump(t *testing.T) {
	setts := s.Settings{
		"keycapacity": 10 * 1024 * 1024, "valcapacity": 10 * 1024 * 1024,
	}
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
	t.Skip("skipping")
	setts := s.Settings{
		"keycapacity": 100 * 1024 * 1024, "valcapacity": 100 * 1024 * 1024,
	}
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

func TestMVCCClone(t *testing.T) {
	t.Skip("skipping")
	setts := s.Settings{
		"keycapacity": 100 * 1024 * 1024, "valcapacity": 100 * 1024 * 1024,
	}
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

func TestMVCCSetCAS(t *testing.T) {
	t.Skip("skipping")
	var err error
	var cas uint64

	setts := s.Settings{
		"keycapacity": 100 * 1024 * 1024, "valcapacity": 100 * 1024 * 1024,
	}
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
	// set with cas
	k, v := []byte("key100"), []byte("valu100")
	oldvalue, cas, err = mvcc.SetCAS(k, v, oldvalue, 101)
	if err != nil {
		t.Error(err)
	} else if string(oldvalue) != "val100" {
		t.Errorf("unexpected %s", oldvalue)
	} else if cas != uint64(n+1) {
		t.Errorf("expected %v, got %v", n+1, cas)
	}
	rvm = rvm - len(oldvalue) + len(v)
	mvcc.Validate()
	// set with invalid cas
	k = []byte("key100")
	oldvalue, cas, err = mvcc.SetCAS(k, nil, oldvalue, 100)
	if err.Error() != api.ErrorInvalidCAS.Error() {
		t.Errorf("expected error")
	}
	mvcc.Validate()
	// set with cas again
	k = []byte("key100")
	oldvalue, cas = mvcc.Delete(k, oldvalue, true /*lsm*/)
	if string(oldvalue) != "valu100" {
		t.Errorf("unexpected %s", oldvalue)
	} else if cas != uint64(n+2) {
		t.Errorf("expected %v, got %v", n+2, cas)
	}
	rvm = rvm - len(oldvalue)
	mvcc.Validate()
	// set with mismatch cas for deleted key.
	k, v = []byte("key100"), []byte("value100")
	oldvalue, cas, err = mvcc.SetCAS(k, v, oldvalue, 100)
	if err.Error() != api.ErrorInvalidCAS.Error() {
		t.Errorf("expected error")
	}
	mvcc.Validate()
	// set with mismatch cas and oldvalue as nil.
	nilvalue := []byte(nil)
	k, v = []byte("key100"), []byte("value100")
	nilvalue, cas, err = mvcc.SetCAS(k, v, nil, 100)
	if err.Error() != api.ErrorInvalidCAS.Error() {
		t.Errorf("expected error")
	} else if nilvalue != nil {
		t.Errorf("unexpected %s", nilvalue)
	}
	mvcc.Validate()
	// set with cas and value nil.
	k, v = []byte("key100"), nil
	oldvalue, cas, err = mvcc.SetCAS(k, v, oldvalue, 1002)
	if err != nil {
		t.Error(err)
	} else if cas != 1003 {
		t.Errorf("unexpected %v", cas)
	} else if string(oldvalue) != "" {
		t.Errorf("unexpected %s", oldvalue)
	}
	mvcc.Validate()
	// set with cas and oldvalue nil.
	k, v = []byte("key100"), []byte("value100")
	nilvalue, cas, err = mvcc.SetCAS(k, v, nil, 1003)
	if err != nil {
		t.Error(err)
	} else if cas != 1004 {
		t.Errorf("unexpected %v", cas)
	} else if nilvalue != nil {
		t.Errorf("unexpected %s", nilvalue)
	}
	rvm = rvm + len(v)
	mvcc.Validate()
	// set with cas and oldvalue nil.
	k, v = []byte("missing"), []byte("value100")
	oldvalue, cas, err = mvcc.SetCAS(k, v, oldvalue, 1003)
	if err.Error() != api.ErrorInvalidCAS.Error() {
		t.Errorf("unexpected %v", err)
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

func TestMVCCDelete(t *testing.T) {
	t.Skip("skipping")
	var err error
	var cas uint64

	setts := s.Settings{
		"keycapacity": 100 * 1024 * 1024, "valcapacity": 100 * 1024 * 1024,
	}
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
	// mutation: delete a valid key
	k, v := []byte("key100"), []byte("val100")
	oldvalue, cas = mvcc.Delete(k, oldvalue, false /*lsm*/)
	if cas != uint64(n+2) {
		t.Errorf("expected %v, got %v", n+2, cas)
	} else if string(oldvalue) != "val100" {
		t.Errorf("unexpected %s", oldvalue)
	}
	rkm, rvm = rkm-len(k), rvm-len(v)
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
	// mutation: set-cas on deleted key
	k, v = []byte("key100"), []byte("valu100")
	oldvalue, cas, err = mvcc.SetCAS(k, v, oldvalue, 0)
	if err != nil {
		t.Error(err)
	} else if cas != uint64(n+3) {
		t.Errorf("expected %v, got %v", n+3, cas)
	}
	rkm, rvm = rkm+len(k), rvm+len(v)
	// mutation: delete with lsm
	k = []byte("key100")
	oldvalue, cas = mvcc.Delete(k, oldvalue, true /*lsm*/)
	if cas != uint64(n+4) {
		t.Errorf("expected %v, got %v", n+4, cas)
	} else if string(oldvalue) != "valu100" {
		t.Errorf("unexpected %s", oldvalue)
	}
	rkm, rvm = rkm-len(k), rvm-len(v)
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
	// mutation: set-cas on deleted key
	k, v = []byte("key100"), []byte("value100")
	oldvalue, cas, err = mvcc.SetCAS(k, v, oldvalue, uint64(n+4))
	if err != nil {
		t.Error(err)
	} else if cas != uint64(n+5) {
		t.Errorf("expected %v, got %v", n+5, cas)
	} else if len(oldvalue) > 0 {
		t.Errorf("unexpected %s", oldvalue)
	}
	rkm, rvm = rkm+len(k), rvm+len(v)
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
		mvcc.Validate()
	}

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

func TestMVCCTxn(t *testing.T) {
	t.Skip("skipping")
	setts := s.Settings{
		"keycapacity": 10 * 1024 * 1024, "valcapacity": 10 * 1024 * 1024,
	}
	mvcc := NewMVCC("txn", setts)
	defer mvcc.Destroy()

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
	txn.Commit()
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
	txn.Commit()
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
	t.Skip("skipping")
	setts := s.Settings{
		"keycapacity": 10 * 1024 * 1024, "valcapacity": 10 * 1024 * 1024,
	}
	mvcc := NewMVCC("view", setts)
	defer mvcc.Destroy()

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
	t.Skip("skipping")
	setts := s.Settings{
		"keycapacity": 10 * 1024 * 1024, "valcapacity": 10 * 1024 * 1024,
	}
	mvcc := NewMVCC("view", setts)
	defer mvcc.Destroy()

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

	txn := mvcc.BeginTxn(0x1234)
	for i, key := range keys {
		cur := txn.OpenCursor([]byte(key))
		testgetnext(t, cur, i, keys, vals)
		if k, _, _ := cur.GetNext(); k != nil {
			t.Errorf("unexpected %s", k)
		}
		cur = txn.OpenCursor([]byte(key))
		testynext(t, cur, i, keys, vals)
		if k, _ := cur.Key(); k != nil {
			t.Errorf("unexpected %s", k)
		} else if v := cur.Value(); v != nil {
			t.Errorf("unexpected %s", v)
		}
	}
	cur := txn.OpenCursor([]byte(keys[0]))
	cur.Delcursor(true /*lsm*/)
	cur.Delete([]byte(keys[1]), nil, true /*lsm*/)
	value := []byte("newvalue")
	cur.Set([]byte(keys[2]), value, nil)
	txn.Commit()

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
	t.Skip("skipping")
	setts := s.Settings{
		"keycapacity": 10 * 1024 * 1024, "valcapacity": 10 * 1024 * 1024,
	}
	mvcc := NewMVCC("view", setts)
	defer mvcc.Destroy()

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

	for i, key := range keys {
		view := mvcc.View(0x1234 + uint64(i))
		cur := view.OpenCursor([]byte(key))
		testgetnext(t, cur, i, keys, vals)
		view.Abort()
		view = mvcc.View(0)
		cur = view.OpenCursor([]byte(key))
		testynext(t, cur, i, keys, vals)
		view.Abort()
	}

	txn := mvcc.BeginTxn(0x12345)
	cur := txn.OpenCursor([]byte(keys[0]))
	cur.Delcursor(true /*lsm*/)
	cur.Delete([]byte(keys[1]), nil, true /*lsm*/)
	value := []byte("newvalue")
	cur.Set([]byte(keys[2]), value, nil)
	txn.Commit()

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

func BenchmarkMVCCCount(b *testing.B) {
	b.Skip("skipping")
	mvcc := makeBenchMVCC(1000)
	defer mvcc.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mvcc.Count()
	}
}

func BenchmarkMVCCSet(b *testing.B) {
	b.Skip("skipping")
	var scratch [8]byte

	setts := s.Settings{
		"keycapacity": 10 * 1024 * 1024, "valcapacity": 10 * 1024 * 1024,
	}
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
	b.Skip("skipping")
	var scratch [8]byte

	setts := s.Settings{
		"keycapacity": 10 * 1024 * 1024, "valcapacity": 10 * 1024 * 1024,
	}
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
	b.Skip("skipping")
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
	b.Skip("skipping")
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
	b.Skip("skipping")
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
	b.Skip("skipping")
	mvcc := makeBenchMVCC(1000)
	defer mvcc.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := mvcc.BeginTxn(0)
		txn.Commit()
	}
}

func BenchmarkMVCCClone(b *testing.B) {
	b.Skip("skipping")
	mvcc := makeBenchMVCC(b.N)
	defer mvcc.Destroy()

	b.ResetTimer()
	mvcc.Clone("benchclone")
}

func BenchmarkMVCCView(b *testing.B) {
	b.Skip("skipping")
	mvcc := makeBenchMVCC(1000)
	defer mvcc.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := mvcc.View(0)
		txn.Abort()
	}
}

func makeBenchMVCC(n int) *MVCC {
	var scratch [8]byte

	setts := s.Settings{
		"keycapacity": 10 * 1024 * 1024, "valcapacity": 10 * 1024 * 1024,
	}
	mvcc := NewMVCC("bench", setts)
	k, v := []byte("key000000000000"), []byte("val00000000000000")
	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(scratch[:], uint64(i+1))
		key, val := append(k[:3], scratch[:]...), append(v[:3], scratch[:]...)
		mvcc.Set(key, val, nil)
	}
	return mvcc
}
