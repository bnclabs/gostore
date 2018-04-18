package lsm

import "io"
import "fmt"
import "bytes"

import "github.com/bnclabs/gostore/api"

var _ = fmt.Sprintf("")

func pulle(
	x api.EntryIterator, fin bool, key []byte,
	eof api.IndexEntry) api.IndexEntry {

	if x == nil {
		return eof
	}

	nextentry := x(fin)
	nextkey, _, _, nexterr := nextentry.Key()
	for nexterr == nil && bytes.Compare(nextkey, key) == 0 {
		//fmt.Printf("skip %v %q %q %v\n", debug, key, nextkey, nexterr)
		nextentry = x(fin)
		nextkey, _, _, nexterr = nextentry.Key()
	}
	return nextentry
}

// YSortEntries is a iterate combinator that takes two iterator and
// return a new iterator that handles LSM.
func YSortEntries(a, b api.EntryIterator) api.EntryIterator {
	var aentry, bentry api.IndexEntry
	var key []byte
	var aseqno, bseqno uint64
	var aerr, berr error

	akey, bkey := make([]byte, 0, 16), make([]byte, 0, 16)
	eof := neweofentry()
	anext, bnext, entry := true, true, api.IndexEntry(eof)

	return func(fin bool) api.IndexEntry {
		if aerr == nil && anext {
			aentry = pulle(a, fin, akey, eof)
			if aentry != nil {
				key, aseqno, _, aerr = aentry.Key()
				akey = cp(akey, key)
				//fmt.Printf("%v aentry %q %v %v %v\n", id, akey, aseqno, adel, aerr)
			} else {
				aerr = io.EOF
			}
		}
		if berr == nil && bnext {
			bentry = pulle(b, fin, bkey, eof)
			if bentry != nil {
				key, bseqno, _, berr = bentry.Key()
				bkey = cp(bkey, key)
				//fmt.Printf("%v bentry %q %v %v %v\n", id, bkey, bseqno, bdel, aerr)
			} else {
				berr = io.EOF
			}
		}

		anext, bnext = false, false
		if aerr != nil && berr != nil {
			entry = eof

		} else if aerr != nil {
			entry, bnext = bentry, true

		} else if berr != nil {
			entry, anext = aentry, true

		} else if cmp := bytes.Compare(bkey, akey); cmp < 0 {
			entry, bnext = bentry, true

		} else if cmp > 0 {
			entry, anext = aentry, true

		} else {
			if bseqno > aseqno {
				entry = bentry
			} else {
				entry = aentry
			}
			anext, bnext = true, true
		}
		return entry
	}
}
