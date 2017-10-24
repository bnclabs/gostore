package llrb

import "github.com/prataprc/golog"
import "github.com/prataprc/gostore/api"

func init() {
	setts := map[string]interface{}{
		"log.level":      "ignore",
		"log.colorfatal": "red",
		"log.colorerror": "hired",
		"log.colorwarn":  "yellow",
	}
	log.SetLogger(nil, setts)

	// check whether mvcc confirms to api.Index{} interface.
	var _ api.Index = NewMVCC("dummy", Defaultsettings())
	// check whether llrb confirms to api.Index{} interface.
	var _ api.Index = NewLLRB("dummy", Defaultsettings())
}
