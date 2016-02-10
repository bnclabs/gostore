package storage

import "fmt"

func (llrb *LLRB) validateConfig(config map[string]interface{}) {
	minblock := config["nodearena.minblock"].(int)
	maxblock := config["nodearena.maxblock"].(int)
	capacity := config["nodearena.capacity"].(int)
	if minblock < MinKeymem {
		fmsg := "nodearena.minblock < %v configuration"
		panic(fmt.Errorf(fmsg, MinKeymem))
	} else if maxblock > MaxKeymem {
		fmsg := "nodearena.maxblock > %v configuration"
		panic(fmt.Errorf(fmsg, MaxKeymem))
	} else if capacity == 0 {
		panic("nodearena.capacity cannot be ZERO")
	}

	minblock = config["valarena.minblock"].(int)
	maxblock = config["valarena.maxblock"].(int)
	capacity = config["valarena.capacity"].(int)
	if minblock < MinValmem {
		fmsg := "valarena.minblock < %v configuration"
		panic(fmt.Errorf(fmsg, MinValmem))
	} else if maxblock > MaxValmem {
		fmsg := "valarena.maxblock > %v configuration"
		panic(fmt.Errorf(fmsg, MaxValmem))
	} else if capacity == 0 {
		panic("valarena.capacity cannot be ZERO")
	}
}

func (llrb *LLRB) newnodearena(config map[string]interface{}) *memarena {
	minblock := int64(config["nodearena.minblock"].(int))
	maxblock := int64(config["nodearena.maxblock"].(int))
	capacity := int64(config["nodearena.capacity"].(int))
	pcapacity := int64(config["nodearena.pool.capacity"].(int))
	return newmemarena(minblock, maxblock, capacity, pcapacity)
}

func (llrb *LLRB) newvaluearena(config map[string]interface{}) *memarena {
	minblock := int64(config["valarena.minblock"].(int))
	maxblock := int64(config["valarena.maxblock"].(int))
	capacity := int64(config["valarena.capacity"].(int))
	pcapacity := int64(config["valarena.pool.capacity"].(int))
	return newmemarena(minblock, maxblock, capacity, pcapacity)
}

func (llrb *LLRB) setupfmask(config map[string]interface{}) metadataMask {
	fmask := metadataMask(0)
	if conf, ok := config["metadata.bornseqno"]; ok && conf.(bool) {
		fmask = fmask.enableBornSeqno()
	}
	if conf, ok := config["metadata.deadseqno"]; ok && conf.(bool) {
		fmask = fmask.enableDeadSeqno()
	}
	if conf, ok := config["metadata.mvalue"]; ok && conf.(bool) {
		fmask = fmask.enableMvalue()
	}
	if conf, ok := config["metadata.vbuuid"]; ok && conf.(bool) {
		fmask = fmask.enableVbuuid()
	}
	return fmask
}
