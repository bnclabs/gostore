package llrb

import "fmt"
import "testing"
import "encoding/json"

import "github.com/prataprc/gostore/lib"
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
	} else if x := stats["n_cursors"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	}

	llrb.Log()
}

func TestLLRBLoad(t *testing.T) {
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
		oldvalue = llrb.Set(k, v, oldvalue)
	}
	// test loaded data
	value := make([]byte, 1024)
	for i, key := range keys {
		if value, ok := llrb.Get(lib.Str2bytes(key), value); !ok {
			t.Errorf("expected key %q", key)
		} else if string(value) != vals[i] {
			t.Errorf("expected %q, got %q, key %q", vals[i], value, key)
		}
	}

	if llrb.Count() != int64(len(keys)) {
		t.Errorf("unexpected %v", llrb.Count())
	}

	// validate
	llrb.Validate()
	stats := llrb.Stats()
	if x := stats["keymemory"].(int64); x != 72 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["valmemory"].(int64); x != 72 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_count"].(int64); x != int64(len(keys)) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_inserts"].(int64); x != int64(len(keys)) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_updates"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_deletes"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_reads"].(int64); x != int64(len(keys)) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_clones"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_frees"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_nodes"].(int64); x != int64(len(keys)) {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_txns"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if x := stats["n_cursors"].(int64); x != 0 {
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
	llrb := NewLLRB1("load", setts)
	defer llrb.Destroy()

	// load data
	n, oldvalue, rkm, rvm := 1000000, make([]byte, 1024), 0, 0
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%v", i))
		v := []byte(fmt.Sprintf("val%v", i))
		oldvalue = llrb.Set(k, v, oldvalue)
		rkm, rvm = rkm+len(k), rvm+len(v)
	}
	// test loaded data
	value := make([]byte, 1024)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		if value, ok := llrb.Get(lib.Str2bytes(key), value); !ok {
			t.Errorf("expected key %q", key)
		} else if string(value) != val {
			t.Errorf("expected %q, got %q, key %q", val, value, key)
		}
	}

	if llrb.Count() != int64(n) {
		t.Errorf("unexpected %v", llrb.Count())
	}

	// validate
	llrb.Validate()
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
	} else if x := stats["n_cursors"].(int64); x != 0 {
		t.Errorf("unexpected %v", x)
	} else if u := nodeutz(stats); u < 50.0 {
		t.Errorf("unexpected %v", u)
	} else if u := valueutz(stats); u < 50.0 {
		t.Errorf("unexpected %v", u)
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
		"log.level":      "info",
		"log.colorfatal": "red",
		"log.colorerror": "hired",
		"log.colorwarn":  "yellow",
	}
	log.SetLogger(nil, setts)
}
