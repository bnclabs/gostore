package bubt

import "fmt"
import "net/http"

import "github.com/prataprc/golog"
import _ "net/http/pprof"

var _ = fmt.Sprintf("dummy")

func init() {
	setts := map[string]interface{}{
		"log.level": "warn",
		"log.file":  "",
	}
	log.SetLogger(nil, setts)
	go func() {
		log.Infof("%v", http.ListenAndServe("localhost:6060", nil))
	}()
}
