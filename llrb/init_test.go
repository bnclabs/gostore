package llrb

import "fmt"
import "net/http"

import "github.com/bnclabs/golog"
import _ "net/http/pprof"

var _ = fmt.Sprintf("dummy")

func init() {
	setts := map[string]interface{}{
		"log.level":      "ignore",
		"log.colorfatal": "red",
		"log.colorerror": "hired",
		"log.colorwarn":  "yellow",
	}
	log.SetLogger(nil, setts)

	go func() {
		log.Infof("%v", http.ListenAndServe("localhost:6060", nil))
	}()
}
