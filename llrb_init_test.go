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
