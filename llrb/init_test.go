package llrb

import "fmt"
import "testing"

import "github.com/prataprc/storage.go/lib"
import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/log"

var _ = fmt.Sprintf("dummy")

func init() {
	config := map[string]interface{}{
		"log.level": "warn",
		"log.file":  "",
	}
	log.SetLogger(nil, config)
}

func TestLLRBValidate(t *testing.T) {
	dotest := func(config lib.Config) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		llrb := NewLLRB("test", config)
		llrb.validateConfig(config)
	}

	config := Defaultconfig()
	config["nodearena.minblock"] = api.MinKeymem - 1
	dotest(config)

	config = Defaultconfig()
	config["nodearena.maxblock"] = api.MaxKeymem + 1
	dotest(config)

	config = Defaultconfig()
	config["nodearena.capacity"] = 0
	dotest(config)

	config = Defaultconfig()
	config["valarena.minblock"] = api.MinValmem - 1
	dotest(config)

	config = Defaultconfig()
	config["valarena.maxblock"] = api.MaxValmem + 1
	dotest(config)

	config = Defaultconfig()
	config["valarena.capacity"] = 0
	dotest(config)
}
