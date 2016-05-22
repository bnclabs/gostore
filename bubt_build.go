// +build ignore

package storage

func (f *bubtstore) build() (rootpos int64) {
	var block bubtblock

	prepend := func(ms []*bubtmblock, mblock *bubtmblock) []*bubtmblock {
		ln, ms = len(ms), append(ms, nil)
		copy(ms[1:], ms[:ln])
		ms[0] = mblock
		return ms
	}

	ms := []*bubtmblock{}
	ms, block = buildm(f, ms)
	for block != nil {
		mblock := f.newm()
		mblock.insert(f.flush(block)) // TODO: insert should handle failure.
		ms, block = buildm(f, prepend(ms, mblock))
	}
	return ms[0].offset()
}

func (f *bubtstore) buildm(ms []*bubtmblock) ([]*bubtmblock, bubtblock) {
	if len(ms) == 0 {
		return ms, buildz(f)
	}

	mblock := ms[0]

	ms, block := buildm(f, ms[1:])
	_, _, ok := mblock.insert(block)
	for ok {
		f.flush(block)
		if ms, block = buildm(f, ms[1:]); block != nil {
			_, _, ok = mblock.insert(block)
			continue
		}
		break
	}
	if block != nil {
		ms[0] = f.newm()
		ms[0].insert(f.flush(block)) // TODO : insert should handle failure
		return ms, mblock
	}
	f.flush(mblock)
	return ms[1:], mblock
}

func (f *bubtstore) buildz() *bubtzblock {
	if zblock := f.newz(); zblock != nil {
		nd := f.pop()
		_, _, ok := zblock.insert(nd)
		for ok {
			nd = f.pop()
			_, _, ok = zblock.insert(nd)
		}
		if nd != nil {
			f.push(nd)
		}
		return zblock
	}
	panic(ErrOutofSpace)
}
