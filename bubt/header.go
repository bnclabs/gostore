package bubt

import "io"
import "fmt"
import "encoding/json"
import "encoding/binary"

import s "github.com/bnclabs/gosettings"
import "github.com/bnclabs/gostore/lib"

func readmarker(r io.ReaderAt) error {
	fsize := filesize(r)
	if fsize < 0 {
		return fmt.Errorf("bubt.snap.nomarker")
	}

	// validate marker block
	fpos := fsize - MarkerBlocksize
	buffer := lib.Fixbuffer(nil, MarkerBlocksize)
	n, err := r.ReadAt(buffer, fpos)
	if err != nil {
		return err
	} else if n < len(buffer) {
		return fmt.Errorf("bubt.snap.partialmarker")
	}
	for _, c := range buffer {
		if c != MarkerByte {
			return fmt.Errorf("bubt.snap.invalidmarker")
		}
	}
	return nil
}

func readmetadata(r io.ReaderAt) (metadata []byte, err error) {
	fsize := filesize(r)
	fpos := fsize - MarkerBlocksize // skip markerblock
	if fpos -= 8; fpos < 0 {
		return nil, fmt.Errorf("bubt.snap.nomdlen")
	}

	var scratch [8]byte
	n, err := r.ReadAt(scratch[:], fpos)
	if err != nil {
		return nil, err
	} else if n < len(scratch) {
		return nil, fmt.Errorf("bubt.snap.partialmdlen")
	}
	mdlen := binary.BigEndian.Uint64(scratch[:])

	if fpos -= int64(mdlen) - 8; fpos < 0 {
		return nil, fmt.Errorf("bubt.snap.nometadata")
	}

	metadata = lib.Fixbuffer(nil, int64(mdlen))
	n, err = r.ReadAt(metadata, fpos)
	if err != nil {
		return nil, err
	} else if n < len(metadata) {
		return nil, fmt.Errorf("bubt.snap.partialmetadata")
	}
	ln := binary.BigEndian.Uint64(metadata)
	metadata = metadata[8 : 8+ln]
	return metadata, nil
}

func readinfoblock(r io.ReaderAt) (fpos int64, info s.Settings, err error) {
	fsize := filesize(r)
	// skip markerblock
	fpos = fsize - MarkerBlocksize
	// skip metadata
	var scratch [8]byte
	n, err := r.ReadAt(scratch[:], fpos-8)
	if err != nil {
		return fpos, nil, err
	} else if n < len(scratch) {
		return fpos, nil, fmt.Errorf("bubt.snap.partialmdlen")
	}
	mdlen := binary.BigEndian.Uint64(scratch[:])
	fpos -= int64(mdlen)

	// position at infoblock
	if fpos -= MarkerBlocksize; fpos < 0 {
		return fpos, nil, fmt.Errorf("bubt.snap.noinfoblock")
	}

	info = s.Settings{}
	buffer := lib.Fixbuffer(nil, MarkerBlocksize)
	n, err = r.ReadAt(buffer, fpos)
	if err != nil {
		return fpos, nil, err
	} else if n < len(buffer) {
		return fpos, nil, fmt.Errorf("bubt.snap.partialinfoblock")
	}
	ln := binary.BigEndian.Uint64(buffer)
	err = json.Unmarshal(buffer[8:8+ln], &info)
	if err != nil {
		return fpos, nil, err
	}
	return fpos, info, err
}
