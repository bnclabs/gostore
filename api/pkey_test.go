package api

import "testing"
import "reflect"

func TestPKSize(t *testing.T) {
	param0 := Keymask(0)
	param1 := param0.enableTxn()
	param2 := param1.enableValue()
	param3 := param2.enableBornseqno()
	param4 := param3.enableDeadseqno()
	param5 := param4.enableUuid()
	masks := []Keymask{param0, param1, param2, param3, param4, param5}
	refs := []int{
		32, 40, 48, 56, 64, 72,
		32, 40, 48, 56, 64, 72,
		32, 40, 48, 56, 64, 72,
		32, 40, 48, 56, 64, 72,
		40, 48, 56, 64, 72, 80,
		40, 48, 56, 64, 72, 80,
		40, 48, 56, 64, 72, 80,
		40, 48, 56, 64, 72, 80,
	}

	for i, j := 0, 0; i < 8; i++ {
		key := make([]byte, 8+i)
		for k, mask := range masks {
			if x := PKSize(key, mask); refs[j] != x {
				t.Log(i, k)
				t.Errorf("expected %v, got %v", refs[j], x)
			}
			j++
		}
	}
}

func TestBytestuff(t *testing.T) {
	in := []byte{0, 1, 1, 0}
	in = append(in, []byte("hello world")...)
	in = append(in, []byte{0, 1, 1, 0}...)
	out := make([]byte, 2*len(in))
	out = keystuff(in, out)
	ref := []byte{1, 0, 1, 1, 1, 1, 1, 0, 104, 101, 108, 108, 111, 32,
		119, 111, 114, 108, 100, 1, 0, 1, 1, 1, 1, 1, 0, 0, 0}
	if reflect.DeepEqual(ref, out) == false {
		t.Errorf("expected %v", ref)
		t.Errorf("got %v", out)
	}
	back, n := keyunstuff(out, make([]byte, len(in)))
	if reflect.DeepEqual(in, back) == false {
		t.Errorf("expected %v, got %v", in, back)
	} else if n != len(out) {
		t.Errorf("expected %v, got %v", len(out), n)
	}
}

func TestKeyhdr(t *testing.T) {
	hdr := Keyhdr(0).setmask(0x80000001).setvbno(0x8001).setflags(0x8001)
	if hdr.getmask() != 0x80000001 {
		t.Errorf("expected %v, got %v", 0x80000001, hdr.getmask())
	} else if hdr.getvbno() != 0x8001 {
		t.Errorf("expected %v, got %v", 0x8001, hdr.getvbno())
	} else if hdr.getflags() != 0x8001 {
		t.Errorf("expected %v, got %v", 0x8001, hdr.getflags())
	}
}

func TestLookupones(t *testing.T) {
	countones := func(b uint8) (c byte) {
		for c = 0; b != 0; b >>= 1 { // count set bits
			c += byte(b & 1)
		}
		return c
	}
	for i, val := range lookupones {
		if countones(uint8(i)) != byte(val) {
			t.Errorf("expected %v, got %v", countones(uint8(i)), val)
		}
	}
}

func TestKeymask(t *testing.T) {
	km := Keymask(0)
	if km.isTxn() == true {
		t.Errorf("unexpected true")
	} else if km.isValue() == true {
		t.Errorf("unexpected true")
	} else if km.isBornseqno() == true {
		t.Errorf("unexpected true")
	} else if km.isDeadseqno() == true {
		t.Errorf("unexpected true")
	} else if km.isUuid() == true {
		t.Errorf("unexpected true")
	} else if km.enableTxn().isTxn() == false {
		t.Errorf("unexpected false")
	} else if km.enableValue().isValue() == false {
		t.Errorf("unexpected false")
	} else if km.enableBornseqno().isBornseqno() == false {
		t.Errorf("unexpected false")
	} else if km.enableDeadseqno().isDeadseqno() == false {
		t.Errorf("unexpected false")
	} else if km.enableUuid().isUuid() == false {
		t.Errorf("unexpected false")
	}
}

func TestKeyflags(t *testing.T) {
	f := Keyflags(0)
	f = f.Setblack().Setdirty().Setdeleted()
	if f.Isblack() == false {
		t.Errorf("unexpected false")
	} else if f.Isdirty() == false {
		t.Errorf("unexpected false")
	} else if f.Isdeleted() == false {
		t.Errorf("unexpected false")
	} else if f.Cleardirty().Isdirty() == true {
		t.Errorf("unexpected true")
	} else if f.Cleardeleted().Isdeleted() == true {
		t.Errorf("unexpected true")
	} else if f.Togglelink().Isblack() == true {
		t.Errorf("unexpected true")
	} else if f.Togglelink().Isred() == false {
		t.Errorf("unexpected false")
	} else if f.Setred().Isred() == false {
		t.Errorf("unexpected false")
	}
}

func BenchmarkBytestuff(b *testing.B) {
	in := make([]byte, 64)
	out := make([]byte, PKSize(in, 0))
	for i := 0; i < b.N; i++ {
		keystuff(in, out)
	}
}

func BenchmarkByteunstuff(b *testing.B) {
	in := make([]byte, 64)
	out := make([]byte, PKSize(in, 0))
	out = keystuff(in, out)
	in, out = out, in
	for i := 0; i < b.N; i++ {
		keyunstuff(in, out)
	}
}
