package main

import "log"
import "fmt"

import "github.com/prataprc/storage.go"

var _ = fmt.Sprintf("dummy")

func mvccreader(readerid int, ropch chan llrbcmd) {
	var dictsnap storage.Snapshot
	var llrbsnap storage.Snapshot
	var genstats map[string]int

	releasesnaps := func() {
		if dictsnap != nil {
			dictsnap.Release()
		}
		if llrbsnap != nil {
			llrbsnap.Release()
		}
		dictsnap, llrbsnap = nil, nil
	}

	for {
		lcmd := <-ropch
		cmdname := lcmd.cmd[0].(string)
		// wait for snapshot
		if cmdname != "snapshot" && (dictsnap == nil || llrbsnap == nil) {
			continue
		}
		//fmt.Println(readerid, cmdname)
		if verifyopts.opdump {
			fmt.Printf("reader %v cmd %v\n", readerid, lcmd.cmd)
		}
		switch cmdname {
		case "get":
			genstats = llrb_opGet(dictsnap, llrbsnap, lcmd, genstats)
		case "min":
			genstats = llrb_opMin(dictsnap, llrbsnap, lcmd, genstats)
		case "max":
			genstats = llrb_opMax(dictsnap, llrbsnap, lcmd, genstats)
		case "range":
			genstats = llrb_opRange(dictsnap, llrbsnap, lcmd, genstats)
		case "validate":
			dolog := lcmd.cmd[1].(bool)
			llrb_opValidate(dictsnap, llrbsnap, genstats, dolog)
		case "snapshot":
			releasesnaps()
			dictsnap = lcmd.cmd[1].(storage.Snapshot)
			llrbsnap = lcmd.cmd[2].(storage.Snapshot)
			genstats = lcmd.cmd[3].(map[string]int)
			snaprespch := lcmd.cmd[4].(chan interface{})
			dictsnap.Refer()
			llrbsnap.Refer()
			snaprespch <- true

		case "release":
			releasesnaps()

		default:
			log.Fatalf("unknown command %v\n", lcmd.cmd)
		}
	}
}
