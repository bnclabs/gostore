package main

import "fmt"
import "os"
import "log"
import "net/http"
import "math/rand"
import _ "net/http/pprof"
import "runtime/pprof"

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:8080", nil))
	}()

	switch os.Args[1] {
	case "load":
		doLoad(os.Args[2:])
	case "validate":
		doValidate(os.Args[2:])
	default:
		fmt.Println("please provide a valid command !!")
	}
}

func makekey(key []byte, min, max int) (k []byte) {
	if max-min > 0 {
		k = key[:rand.Intn(max-min)+min]
		for i := range k {
			k[i] = byte(97 + rand.Intn(26))
		}
	}
	return
}

func makeval(val []byte, min, max int) (v []byte) {
	if max-min > 0 {
		v = val[:rand.Intn(max-min)+min]
		for i := range v {
			v[i] = byte(97 + rand.Intn(26))
		}
	}
	return
}

func takeMEMProfile(filename string) bool {
	if filename == "" {
		return false
	}
	fd, err := os.Create(filename)
	if err != nil {
		fmt.Println(err)
		return false
	}
	pprof.WriteHeapProfile(fd)
	defer fd.Close()
	return true
}
