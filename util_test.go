package storage

import "testing"
import "fmt"
import "errors"

var _ = fmt.Sprintf("dummy")

func TestFindFSet8(t *testing.T) {
	if x := findfirstset8(uint8(0)); x != -1 {
		t.Errorf("expected %v, got %v", -1, x)
	} else if x = findfirstset8(uint8(0x80)); x != 7 {
		t.Errorf("expected %v, got %v", 7, x)
	} else if x = findfirstset8(uint8(0x10)); x != 4 {
		t.Errorf("expected %v, got %v", 4, x)
	}
}

func TestClearbit8(t *testing.T) {
	for i := uint8(0); i < 8; i++ {
		if x := clearbit8(uint8(1<<i), uint8(i)); x != 0 {
			t.Errorf("expected %v, got %v", 0, x)
		}
	}
}

func TestSetbit8(t *testing.T) {
	for i := uint8(0); i < 8; i++ {
		if x := setbit8(0, uint8(i)); x != uint8(1<<i) {
			t.Errorf("expected %v, got %v", uint8(1<<i), x)
		}
	}
}

func TestZerosin8(t *testing.T) {
	if x := zerosin8(0); x != 8 {
		t.Errorf("expected %v, got %v", 8, x)
	} else if x = zerosin8(0xaa); x != 4 {
		t.Errorf("expected %v, got %v", 4, x)
	} else if x = zerosin8(0x55); x != 4 {
		t.Errorf("expected %v, got %v", 4, x)
	}
}

func TestZerosin32(t *testing.T) {
	if x := zerosin32(0); x != 32 {
		t.Errorf("expected %v, got %v", 0, x)
	} else if x := zerosin32(1); x != 31 {
		t.Errorf("expected %v, got %v", 32, x)
	} else if x = zerosin32(0xaaaaaaaa); x != 16 {
		t.Errorf("expected %v, got %v", 16, x)
	} else if x = zerosin32(0x55555555); x != 16 {
		t.Errorf("expected %v, got %v", 16, x)
	}
}

func TestFailsafeRequest(t *testing.T) {
	reqch := make(chan []interface{}, 10)
	respch := make(chan []interface{}, 1)
	finch := make(chan bool)
	donech := make(chan bool)

	go func() {
		resp, err := failsafeRequest(reqch, respch, []interface{}{"case1"}, finch)
		if err != nil {
			t.Errorf("case1: %v", err)
		} else if resp[0].(string) != "ok1" {
			t.Errorf("expected %v, got %v\n", "ok1", resp[0].(string))
		}
		donech <- true
	}()
	if cmd := <-reqch; cmd[0].(string) != "case1" {
		t.Errorf("expected %v, got %v\n", "case1", cmd[0])
	} else {
		respch <- []interface{}{"ok1"}
	}
	<-donech

	go func() {
		resp, err := failsafeRequest(reqch, respch, []interface{}{"case2"}, finch)
		if resp != nil {
			t.Errorf("expected nil, got %v\n", resp)
		} else if err.Error() != "server closed" {
			t.Errorf("expected server closed, got %v\n", err.Error())
		}
		donech <- true
	}()
	if cmd := <-reqch; cmd[0].(string) != "case2" {
		t.Errorf("expected %v, got %v\n", "case2", cmd[0])
	}
	close(finch)
	<-donech

	go func() {
		resp, err := failsafeRequest(reqch, respch, []interface{}{"case3"}, finch)
		if resp != nil {
			t.Errorf("expected nil, got %v\n", resp)
		} else if err.Error() != "server closed" {
			t.Errorf("expected server closed, got %v\n", err.Error())
		}
		donech <- true
	}()
	<-donech
}

func TestFailsafePost(t *testing.T) {
	reqch := make(chan []interface{}, 10)
	finch := make(chan bool)
	donech := make(chan bool)

	go func() {
		err := failsafePost(reqch, []interface{}{"post1"}, finch)
		if err != nil {
			t.Errorf("post1: %v", err)
		}
		donech <- true
	}()
	if cmd := <-reqch; cmd[0].(string) != "post1" {
		t.Errorf("expected %v, got %v\n", "post1", cmd[0])
	}
	<-donech

	close(finch)
	go func() {
		err := failsafePost(nil, []interface{}{"post2"}, finch)
		if err.Error() != "closed" {
			t.Errorf("expected closed, got %v\n", err.Error())
		}
		donech <- true
	}()
	<-donech
}

func TestResponseError(t *testing.T) {
	err := errors.New("error")
	resp := []interface{}{err, errors.New("resperr")}
	if e := responseError(err, resp, 0).Error(); e != "error" {
		t.Errorf("expected error, got %v", e)
	}
	resp = []interface{}{err, errors.New("resperr")}
	if e := responseError(nil, resp, 1).Error(); e != "resperr" {
		t.Errorf("expected resperr, got %v", e)
	}
}

func BenchmarkFindFSet8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		findfirstset8(0x80)
	}
}

func BenchmarkClearbit8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		clearbit8(0x80, 7)
	}
}

func BenchmarkSetbit8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		setbit8(0x80, 7)
	}
}

func BenchmarkZerosin8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		zerosin8(0xaa)
	}
}

func BenchmarkZerosin32(b *testing.B) {
	for i := 0; i < b.N; i++ {
		zerosin32(0xaaaaaaaa)
	}
}
