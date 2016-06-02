package llrb

import "fmt"
import "testing"

import "github.com/prataprc/storage.go/lib"
import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/log"
import "github.com/prataprc/storage.go/malloc"

var _ = fmt.Sprintf("dummy")

func init() {
	config := map[string]interface{}{
		"log.level": "warn",
		"log.file":  "",
	}
	memratio = 0.04
	log.SetLogger(nil, config)
}

func TestLLRBValidate(t *testing.T) {
	dotest := func(config lib.Config) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		llrb := NewLLRB("test", config, nil)
		llrb.validateConfig(config)
	}

	config := makellrbconfig()
	config["nodearena.minblock"] = api.MinKeymem - 1
	dotest(config)

	config = makellrbconfig()
	config["nodearena.maxblock"] = api.MaxKeymem + 1
	dotest(config)

	config = makellrbconfig()
	config["nodearena.capacity"] = 0
	dotest(config)

	config = makellrbconfig()
	config["valarena.minblock"] = api.MinValmem - 1
	dotest(config)

	config = makellrbconfig()
	config["valarena.maxblock"] = api.MaxValmem + 1
	dotest(config)

	config = makellrbconfig()
	config["valarena.capacity"] = 0
	dotest(config)
}

func makellrbconfig() lib.Config {
	config := lib.Config{
		"maxvb":                   int64(1024),
		"mvcc.enable":             false,
		"mvcc.snapshot.tick":      int64(5), // 5 millisecond
		"mvcc.writer.chanbuffer":  int64(1000),
		"nodearena.minblock":      int64(96),
		"nodearena.maxblock":      int64(1024),
		"nodearena.capacity":      int64(1024 * 1024 * 1024),
		"nodearena.pool.capacity": int64(2 * 1024 * 1024),
		"nodearena.maxpools":      malloc.Maxpools,
		"nodearena.maxchunks":     malloc.Maxchunks,
		"nodearena.allocator":     "flist",
		"valarena.minblock":       int64(96),
		"valarena.maxblock":       int64(1024 * 1024),
		"valarena.capacity":       int64(10 * 1024 * 1024 * 1024),
		"valarena.pool.capacity":  int64(10 * 2 * 1024 * 1024),
		"valarena.maxpools":       malloc.Maxpools,
		"valarena.maxchunks":      malloc.Maxchunks,
		"valarena.allocator":      "flist",
		"log.level":               "ignore",
	}
	return config
}
