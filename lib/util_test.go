package lib

import "testing"
import "fmt"
import "reflect"
import "unsafe"
import "errors"
import "bytes"

var _ = fmt.Sprintf("dummy")

func TestMemcpy(t *testing.T) {
	src, dst := make([]byte, 100), make([]byte, 1024)
	for i := 0; i < len(src); i++ {
		src[0] = 0xAB
	}
	n := Memcpy(
		unsafe.Pointer(((*reflect.SliceHeader)(unsafe.Pointer(&dst))).Data),
		unsafe.Pointer(((*reflect.SliceHeader)(unsafe.Pointer(&src))).Data),
		len(src))
	if n != len(src) {
		t.Fatalf("expected %v, got %v", len(src), n)
	} else if bytes.Compare(dst[:len(src)], src) != 0 {
		t.Fatalf("Memcpy() failed")
	}

	dst, src = make([]byte, 100), make([]byte, 1024)
	for i := 0; i < len(src); i++ {
		src[0] = 0xAB
	}
	n = Memcpy(
		unsafe.Pointer(((*reflect.SliceHeader)(unsafe.Pointer(&dst))).Data),
		unsafe.Pointer(((*reflect.SliceHeader)(unsafe.Pointer(&src))).Data),
		len(dst))
	if n != len(dst) {
		t.Fatalf("expected %v, got %v", len(dst), n)
	} else if bytes.Compare(dst, src[:len(dst)]) != 0 {
		t.Fatalf("Memcpy() failed")
	}
}

func TestFailsafeRequest(t *testing.T) {
	reqch := make(chan []interface{}, 10)
	respch := make(chan []interface{}, 1)
	finch := make(chan bool)
	donech := make(chan bool)

	go func() {
		resp, err := FailsafeRequest(reqch, respch, []interface{}{"case1"}, finch)
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
		resp, err := FailsafeRequest(reqch, respch, []interface{}{"case2"}, finch)
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
		resp, err := FailsafeRequest(reqch, respch, []interface{}{"case3"}, finch)
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
		err := FailsafePost(reqch, []interface{}{"post1"}, finch)
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
		err := FailsafePost(nil, []interface{}{"post2"}, finch)
		if err.Error() != "closed" {
			t.Errorf("expected closed, got %v\n", err.Error())
		}
		donech <- true
	}()
	<-donech
}

func TestResponseError(t *testing.T) {
	err := errors.New("error")
	resp := []interface{}{err, errors.New("resperr"), nil}

	if e := ResponseError(err, resp, 0).Error(); e != "error" {
		t.Errorf("expected error, got %v", e)
	}
	if e := ResponseError(nil, resp, 1).Error(); e != "resperr" {
		t.Errorf("expected resperr, got %v", e)
	}
	if e := ResponseError(nil, resp, 2); e != nil {
		t.Errorf("expected nil, got %v", e)
	}
	if e := ResponseError(nil, nil, 2); e != nil {
		t.Errorf("expected nil, got %v", e)
	}
}

func TestBytes2str(t *testing.T) {
	in := []byte("input")
	if out := Bytes2str(in); out != "input" {
		t.Errorf("expected `input`, got %v", out)
	} else if out = Bytes2str(nil); out != "" {
		t.Errorf("expected ``, got %v", out)
	}
}

func TestStr2Bytes(t *testing.T) {
	in := "input"
	if out := Str2bytes(in); bytes.Compare(out, []byte("input")) != 0 {
		t.Errorf("expected `input`, got %s", out)
	} else if out = Str2bytes(""); out != nil {
		t.Errorf("expected nil, got %s", out)
	}
}

func TestAbsInt64(t *testing.T) {
	if x := AbsInt64(10); x != 10 {
		t.Errorf("expected 10, got %v", x)
	} else if x = AbsInt64(0); x != 0 {
		t.Errorf("expected 0, got %v", x)
	} else if x = AbsInt64(-0); x != 0 {
		t.Errorf("expected 0, got %v", x)
	} else if x = AbsInt64(-10); x != 10 {
		t.Errorf("expected 10, got %v", x)
	}
}

func TestFixbuffer(t *testing.T) {
	if ln := len(Fixbuffer(nil, 10)); ln != 10 {
		t.Errorf("expected %v, got %v", 10, ln)
	} else if ln = len(Fixbuffer(nil, 0)); ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	} else if ln = len(Fixbuffer([]byte{10, 20}, 0)); ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}
}

func TestParsecsv(t *testing.T) {
	res := Parsecsv("a, b, r , x\n, \ty \n")
	ref := []string{"a", "b", "r", "x", "y"}
	if !reflect.DeepEqual(res, ref) {
		t.Errorf("expected %v, got %v", ref, res)
	}
}

func BenchmarkMemcpy(b *testing.B) {
	ln := 10 * 1024
	src, dst := make([]byte, ln), make([]byte, ln)
	for i := 0; i < len(src); i++ {
		src[0] = 0xAB
	}
	for i := 0; i < b.N; i++ {
		Memcpy(
			unsafe.Pointer(((*reflect.SliceHeader)(unsafe.Pointer(&dst))).Data),
			unsafe.Pointer(((*reflect.SliceHeader)(unsafe.Pointer(&src))).Data),
			ln)
	}
}
