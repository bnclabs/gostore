package lib

import "testing"
import "fmt"

var _ = fmt.Sprintf("dummy")

func TestNewuuid(t *testing.T) {
	// check len of uuid
	for i := 8; i < 128; i += 2 {
		if uuid, err := Newuuid(make([]byte, i)); err != nil {
			t.Error(err)
		} else if len(uuid) != i {
			t.Errorf("expected %v, got %v", i, len(uuid))
		}
	}
	// check whether uuids are unique
	ref := map[string]bool{}
	for i := 1; i < 1024*1024; i++ {
		if uuid, err := Newuuid(make([]byte, 8)); err != nil {
			t.Error(err)
		} else if len(uuid) != 8 {
			t.Errorf("expected %v, got %v", 8, len(uuid))
		} else {
			suuid := string(uuid)
			if _, ok := ref[suuid]; ok {
				t.Errorf("%v is not unique, already generated", uuid)
			}
			ref[suuid] = true
		}
	}
	// check negative case
	if _, err := Newuuid(make([]byte, 7)); err == nil {
		t.Errorf("expected error")
	} else if _, err := Allocuuid(3); err == nil {
		t.Errorf("expected error")
	}
}

func TestFormatuuid(t *testing.T) {
	suuid := make([]byte, 1024)

	uuid := Uuid{0x12, 0x23}
	n := uuid.format2(suuid)
	if ref, outs := "1223", string(suuid[:n]); ref != outs {
		t.Errorf("expected %v, got %v", ref, outs)
	}

	uuid = Uuid{0x12, 0x23, 0x34, 0x45, 0x56, 0x67, 0x78, 0x89}
	n = uuid.Format(suuid)
	if ref, outs := "1223344556677889", string(suuid[:n]); ref != outs {
		t.Errorf("expected %v, got %v", ref, outs)
	}

	uuid = Uuid{0x12, 0x23, 0x34, 0x45, 0x56, 0x67, 0x78, 0x89, 0x12, 0x34}
	n = uuid.Format(suuid)
	if ref, outs := "12233445566778891234", string(suuid[:n]); ref != outs {
		t.Errorf("expected %v, got %v", ref, outs)
	}

	uuid = Uuid{
		0x12, 0x23, 0x34, 0x45, 0x56, 0x67, 0x78, 0x89, 0x12, 0x34,
		0x45, 0x56,
	}
	n = uuid.Format(suuid)
	if ref, outs := "122334455667788912344556", string(suuid[:n]); ref != outs {
		t.Errorf("expected %v, got %v", ref, outs)
	}

	uuid = Uuid{
		0x12, 0x23, 0x34, 0x45, 0x56, 0x67, 0x78, 0x89, 0x12, 0x34,
		0x45, 0x56, 0x67, 0x78,
	}
	n = uuid.Format(suuid)
	ref, outs := "1223344556677889123445566778", string(suuid[:n])
	if ref != outs {
		t.Errorf("expected %v, got %v", ref, outs)
	}

	for i := 0; i < 128; i += 2 {
		if uuid, err := Newuuid(make([]byte, i)); err != nil {
			t.Error(err)
		} else if len(uuid) != i {
			t.Errorf("expected %v, got %v", i, len(uuid))
		} else {
			ref := fmt.Sprintf("%x", uuid)
			n = uuid.Format(suuid)
			outs := string(suuid[:n])
			if ref != outs {
				t.Errorf("expected %v, got %v", ref, outs)
			}
		}
	}
}

func TestAllocuuid(t *testing.T) {
	for i := 8; i < 128; i += 2 {
		uuid, err := Allocuuid(i)
		if err != nil {
			t.Error(err)
		} else if i != len(uuid) {
			t.Errorf("expected %v, got %v", i, len(uuid))
		}
	}
}

func BenchmarkNewuuid(b *testing.B) {
	buf := Uuid(make([]byte, 16))
	for i := 0; i < b.N; i++ {
		Newuuid(buf)
	}
}

func BenchmarkFormat(b *testing.B) {
	buf, suuid := Uuid(make([]byte, 16)), make([]byte, 1024)
	uuid, _ := Newuuid(buf)
	for i := 0; i < b.N; i++ {
		uuid.Format(suuid)
	}
}
