package bubt

import "testing"
import "math/rand"
import "fmt"

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

func TestBubtbuild(t *testing.T) {
	lconfig := llrb.Defaultconfig()
	lconfig["metadata.bornseqno"] = true
	lconfig["metadata.deadseqno"] = true
	lconfig["metadata.vbuuid"] = true
	lconfig["metadata.fpos"] = true
	llrb := llrb.NewLLRB("bubttest", lconfig)

	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	// insert 100K items
	count := 100 * 1000
	key, value := make([]byte, 100), make([]byte, 100)
	for i := 0; i < count; i++ {
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, newnd, oldnd api.Node) {
				if oldnd != nil {
					t.Errorf("expected nil")
				} else if x := index.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				if i%3 == 0 {
					seqno++
					newnd.SetDeadseqno(seqno)
				}
			})
		seqno++
	}
	defer llrb.Destroy()
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
