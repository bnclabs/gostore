package llrb

import "fmt"
import "testing"

import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"

var _ = fmt.Sprintf("dummy")

func init() {
	setts := map[string]interface{}{
		"log.level": "warn",
		"log.file":  "",
	}
	log.SetLogger(nil, setts)
}

func TestLLRBValidate(t *testing.T) {
	dotest := func(setts s.Settings) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		NewLLRB("test", setts)
	}

	setts := testsetts(Defaultsettings())
	setts["nodearena.capacity"] = 0
	dotest(setts)

	setts = testsetts(Defaultsettings())
	setts["valarena.capacity"] = 0
	dotest(setts)
}
