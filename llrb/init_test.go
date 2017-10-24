package llrb

import "fmt"
import "net/http"

import "github.com/prataprc/golog"
import _ "net/http/pprof"

var _ = fmt.Sprintf("dummy")

func init() {
	go func() {
		log.Infof("%v", http.ListenAndServe("localhost:6060", nil))
	}()
}
