package bubt

import "fmt"
import "net/http"

import "github.com/bnclabs/golog"
import _ "net/http/pprof"

var _ = fmt.Sprintf("dummy")

func init() {
	setts := map[string]interface{}{
		"log.level": "warn",
		"log.file":  "",
	}
	log.SetLogger(nil, setts)
	LogComponents("self")
	go func() {
		log.Infof("%v", http.ListenAndServe("localhost:6060", nil))
	}()
}
