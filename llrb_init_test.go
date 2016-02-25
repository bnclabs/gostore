package storage

import "fmt"
import "testing"

var _ = fmt.Sprintf("dummy")

func TestLLRBValidate(t *testing.T) {
	dotest := func(config map[string]interface{}) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		llrb := NewLLRB("test", config, nil)
		llrb.validateConfig(config)
	}

	config := makellrbconfig()
	config["nodearena.minblock"] = MinKeymem - 1
	dotest(config)
	config = makellrbconfig()
	config["nodearena.maxblock"] = MaxKeymem + 1
	dotest(config)
	config = makellrbconfig()
	config["nodearena.capacity"] = 0
	dotest(config)
	config = makellrbconfig()
	config["valarena.minblock"] = MinValmem - 1
	dotest(config)
	config = makellrbconfig()
	config["valarena.maxblock"] = MaxValmem + 1
	dotest(config)
	config = makellrbconfig()
	config["valarena.capacity"] = 0
	dotest(config)
}

func makellrbconfig() map[string]interface{} {
	config := map[string]interface{}{
		"maxvb":                   1024,
		"mvcc.enabled":            false,
		"mvcc.snapshot.tick":      5, // 5 millisecond
		"mvcc.writer.chanbuffer":  1000,
		"nodearena.minblock":      96,
		"nodearena.maxblock":      1024,
		"nodearena.capacity":      1024 * 1024 * 1024,
		"nodearena.pool.capacity": 2 * 1024 * 1024,
		"valarena.minblock":       96,
		"valarena.maxblock":       1024 * 1024,
		"valarena.capacity":       10 * 1024 * 1024 * 1024,
		"valarena.pool.capacity":  10 * 2 * 1024 * 1024,
		"log.level":               "ignore",
	}
	return config
}
