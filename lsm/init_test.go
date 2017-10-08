package lsm

import "github.com/prataprc/golog"

func init() {
	setts := map[string]interface{}{
		"log.level": "info",
		"log.file":  "",
	}
	log.SetLogger(nil, setts)
}
