package main

import "log"
import "fmt"

import "github.com/prataprc/storage.go"

var _ = fmt.Sprintf("dummy")

func mvccreader(ropch chan llrbcmd) {
	var dictsnap storage.Snapshot
	var llrbsnap storage.Snapshot
	var genstats map[string]int

	for {
		lcmd := <-ropch
		cmdname := lcmd.cmd[0].(string)
		if cmdname != "snapshot" && (dictsnap == nil || llrbsnap == nil) {
			continue
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
			dictsnap = lcmd.cmd[1].(storage.Snapshot)
			llrbsnap = lcmd.cmd[2].(storage.Snapshot)
			genstats = clonestats(lcmd.cmd[3].(map[string]int))
		case "release":
			dictsnap.Release()
			llrbsnap.Release()
			dictsnap, llrbsnap = nil, nil
		default:
			log.Fatalf("unknown command %v\n", lcmd.cmd)
		}
	}
}
