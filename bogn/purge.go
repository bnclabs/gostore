package bogr

func (bogr *Bogn) purgeindex(index interface{}) {
	bogr.purgech <- index
}

func purger(purgech chan interface{}) {
	for item := range purgech {
		switch index := item.(type) {
		case *llrb.LLRB:
			item.Destroy()
		case *bubt.Snapshot:
			item.Destroy()
		}
	}
}
