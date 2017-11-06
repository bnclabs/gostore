package bogn

import "os"
import "fmt"
import "strings"
import "net/http"
import "path/filepath"

import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"
import _ "net/http/pprof"

var _ = fmt.Sprintf("dummy")

func init() {
	setts := map[string]interface{}{
		"log.level":      "info",
		"log.colorfatal": "red",
		"log.colorerror": "hired",
		"log.colorwarn":  "yellow",
		"log.flags":      "lshortfile",
		"log.timeformat": "",
		"log.prefix":     "",
	}
	log.SetLogger(nil, setts)

	go func() {
		log.Infof("%v", http.ListenAndServe("localhost:6060", nil))
	}()
}

func makepaths() string {
	path, paths := os.TempDir(), []string{}
	for _, base := range []string{"1", "2", "3"} {
		paths = append(paths, filepath.Join(path, base))
	}
	return strings.Join(paths, ",")
}

func makesettings() s.Settings {
	setts := Defaultsettings()
	return setts
}

func destoryindex(name, paths string) {
	setts := Defaultsettings()
	setts["bubt.diskpaths"] = paths
	PurgeIndex(name, setts)
}
