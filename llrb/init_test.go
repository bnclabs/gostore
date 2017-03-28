package llrb

import "fmt"
import "testing"

import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/log"

var _ = fmt.Sprintf("dummy")

func init() {
	setts := map[string]interface{}{
		"log.level": "warn",
		"log.file":  "",
	}
	log.SetLogger(nil, setts)
}

func TestLLRBValidate(t *testing.T) {
	dotest := func(setts lib.Settings) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		NewLLRB("test", setts)
	}

	setts := Defaultsettings()
	setts["maxkeysize"] = api.MaxKeymem + 1
	dotest(setts)

	setts = Defaultsettings()
	setts["nodearena.capacity"] = 0
	dotest(setts)

	setts = Defaultsettings()
	setts["maxvalsize"] = api.MaxValmem + 1
	dotest(setts)

	setts = Defaultsettings()
	setts["valarena.capacity"] = 0
	dotest(setts)
}
