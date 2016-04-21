package storage

import "os"
import "strings"
import "io/ioutil"
import "testing"

// TestSetLogger is moved to this file to make to prevent race situation due
// to SetLogger() call.

func TestSetLogger(t *testing.T) {
	logfile := "setlogger_test.log.file"
	logline := "hello world"
	defer os.Remove(logfile)

	ref := &defaultLogger{level: logLevelIgnore, output: nil}
	log := SetLogger(ref, nil).(*defaultLogger)
	if log.level != logLevelIgnore || log.output != nil {
		t.Errorf("expected %v, got %v", ref, log)
	}

	// test a custom logger
	config := map[string]interface{}{
		"log.level": "info",
		"log.file":  logfile,
	}
	clog := SetLogger(nil, config)
	clog.Infof(logline)
	clog.Verbosef(logline)
	clog.Fatalf(logline)
	clog.Errorf(logline)
	clog.Warnf(logline)
	clog.Tracef(logline)
	if data, err := ioutil.ReadFile(logfile); err != nil {
		t.Error(err)
	} else if s := string(data); !strings.Contains(s, "hello world") {
		t.Errorf("expected %v, got %v", logline, s)
	} else if len(strings.Split(s, "\n")) != 1 {
		t.Errorf("expected %v, got %v", logline, s)
	}
}

func TestAverageInt(t *testing.T) {
	avg := &averageInt64{}
	for i := 1; i <= 100; i++ {
		avg.add(int64(i))
	}
	if x, y := int64(1), avg.min(); x != y {
		t.Errorf("min() expected %v, got %v", x, y)
	} else if x, y := int64(100), avg.max(); x != y {
		t.Errorf("max() expected %v, got %v", x, y)
	} else if x, y := int64(100), avg.samples(); x != y {
		t.Errorf("samples() expected %v, got %v", x, y)
	} else if x, y := int64(100*101)/2, avg.total(); x != y {
		t.Errorf("total() expected %v, got %v", x, y)
	} else if x, y := avg.total()/avg.samples(), avg.mean(); x != y {
		t.Errorf("mean() expected %v, got %v", x, y)
	} else if x, y := 883.5, avg.variance(); x != y {
		t.Errorf("variance() expected %v, got %v", x, y)
	} else if x, y := 29.723727895403698, avg.sd(); x != y {
		t.Errorf("sd() expected %v, got %v", x, y)
	}
}

func BenchmarkAvgintAdd(b *testing.B) {
	avg := &averageInt64{}
	for i := 0; i <= b.N; i++ {
		avg.add(int64(i))
	}
}

func BenchmarkAvgintCount(b *testing.B) {
	avg := &averageInt64{}
	for i := 0; i <= b.N; i++ {
		avg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.samples()
	}
}

func BenchmarkAvgintSum(b *testing.B) {
	avg := &averageInt64{}
	for i := 0; i <= b.N; i++ {
		avg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.total()
	}
}

func BenchmarkAvgintMean(b *testing.B) {
	avg := &averageInt64{}
	for i := 0; i <= b.N; i++ {
		avg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.mean()
	}
}

func BenchmarkAvgintVar(b *testing.B) {
	avg := &averageInt64{}
	for i := 0; i <= b.N; i++ {
		avg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.variance()
	}
}

func BenchmarkAvgintSd(b *testing.B) {
	avg := &averageInt64{}
	for i := 0; i <= b.N; i++ {
		avg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.sd()
	}
}

func BenchmarkAvgclone(b *testing.B) {
	avg := &averageInt64{}
	for i := 0; i <= 1000; i++ {
		avg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.clone()
	}
}
