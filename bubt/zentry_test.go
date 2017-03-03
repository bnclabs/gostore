package bubt

import "testing"

func TestZEntry(t *testing.T) {
	ze := zentry(make([]byte, 1024))

	vbno, vbuuid := uint16(0x1234), uint64(0xffff12345678ffff)
	bornseqno := uint64(0xabababababababab)
	deadseqno := uint64(0xcdcdcdcdcdcdcdcd)
	valnum := uint64(0xefefefefefefefef)
	key := "longkeyabcasdfaksdfja;lsdkfjowireulsadkfjasd;lfkajs;ldfkjasdfkf"

	// set fields
	ze.setflags(zentryFlags(ze.getflags()).setvalfile().setdeleted())
	ze.setvbno(vbno).setvbuuid(vbuuid)
	ze.setbornseqno(bornseqno).setdeadseqno(deadseqno)
	ze.setkey([]byte(key))
	ze.setvaluenum(valnum)

	// validate
	if ze.getflags().isvalfile() == false {
		t.Errorf("expected true")
	} else if ze.getflags().isdeleted() == false {
		t.Errorf("expected true")
	} else if x := ze.getvbno(); x != vbno {
		t.Errorf("expected %v, got %v", vbno, x)
	} else if y := ze.getvbuuid(); y != vbuuid {
		t.Errorf("expected %v, got %v", vbuuid, y)
	} else if y = ze.getbornseqno(); y != bornseqno {
		t.Errorf("expected %v, got %v", bornseqno, y)
	} else if y = ze.getdeadseqno(); y != deadseqno {
		t.Errorf("expected %v, got %v", deadseqno, y)
	} else if k := ze.key(); string(key) != key {
		t.Errorf("expected %v, got %v", key, k)
	} else if v := ze.valuenum(); v != valnum {
		t.Errorf("expected %v, got %v", valnum, v)
	}
}
