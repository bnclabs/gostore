//  Copyright (c) 2014 Couchbase, Inc.

package log

import "io"
import "os"
import "fmt"
import "time"
import "strings"

func init() {
	setts := map[string]interface{}{
		"log.level": "info",
		"log.file":  "",
	}
	SetLogger(nil, setts)
}

// Logger interface for gofast logging, applications can
// supply a logger object implementing this interface or
// gofast will fall back to the defaultLogger{}.
type Logger interface {
	SetLogLevel(string)
	Fatalf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Verbosef(format string, v ...interface{})
	Debugf(format string, v ...interface{})
	Tracef(format string, v ...interface{})
	Printlf(loglevel LogLevel, format string, v ...interface{})
}

// LogLevel defines storage log level.
type LogLevel int

const (
	logLevelIgnore LogLevel = iota + 1
	logLevelFatal
	logLevelError
	logLevelWarn
	logLevelInfo
	logLevelVerbose
	logLevelDebug
	logLevelTrace
)

var log Logger // object used by gofast component for logging.

// SetLogger to integrate storage logging with application logging.
// importing this package will initialize the logger with info level
// logging to console.
func SetLogger(logger Logger, setts map[string]interface{}) Logger {
	if logger != nil {
		log = logger
		return log
	}

	var err error
	level := string2logLevel(setts["log.level"].(string))
	logfd := os.Stdout
	if logfile := setts["log.file"].(string); logfile != "" {
		logfd, err = os.OpenFile(logfile, os.O_RDWR|os.O_APPEND, 0660)
		if err != nil {
			if logfd, err = os.Create(logfile); err != nil {
				panic(err)
			}
		}
	}
	log = &defaultLogger{level: level, output: logfd}
	return log
}

// defaultLogger with default log-file as os.Stdout and,
// default log-level as logLevelInfo. Applications can
// supply a Logger{} object when instantiating the
// Transport.
type defaultLogger struct {
	level  LogLevel
	output io.Writer
}

func (l *defaultLogger) SetLogLevel(level string) {
	l.level = string2logLevel(level)
}

func (l *defaultLogger) Fatalf(format string, v ...interface{}) {
	l.Printlf(logLevelFatal, format, v...)
}

func (l *defaultLogger) Errorf(format string, v ...interface{}) {
	l.Printlf(logLevelError, format, v...)
}

func (l *defaultLogger) Warnf(format string, v ...interface{}) {
	l.Printlf(logLevelWarn, format, v...)
}

func (l *defaultLogger) Infof(format string, v ...interface{}) {
	l.Printlf(logLevelInfo, format, v...)
}

func (l *defaultLogger) Verbosef(format string, v ...interface{}) {
	l.Printlf(logLevelVerbose, format, v...)
}

func (l *defaultLogger) Debugf(format string, v ...interface{}) {
	l.Printlf(logLevelDebug, format, v...)
}

func (l *defaultLogger) Tracef(format string, v ...interface{}) {
	l.Printlf(logLevelTrace, format, v...)
}

func (l *defaultLogger) Printlf(level LogLevel, format string, v ...interface{}) {
	if l.canlog(level) {
		ts := time.Now().Format("2006-01-02T15:04:05.999Z-07:00")
		fmt.Fprintf(l.output, ts+" ["+level.String()+"] "+format, v...)
	}
}

func (l *defaultLogger) canlog(level LogLevel) bool {
	if level <= l.level {
		return true
	}
	return false
}

func (l LogLevel) String() string {
	switch l {
	case logLevelIgnore:
		return "Ignor"
	case logLevelFatal:
		return "Fatal"
	case logLevelError:
		return "Error"
	case logLevelWarn:
		return "Warng"
	case logLevelInfo:
		return "Infom"
	case logLevelVerbose:
		return "Verbs"
	case logLevelDebug:
		return "Debug"
	case logLevelTrace:
		return "Trace"
	}
	panic("unexpected log level") // should never reach here
}

func string2logLevel(s string) LogLevel {
	s = strings.ToLower(s)
	switch s {
	case "ignore":
		return logLevelIgnore
	case "fatal":
		return logLevelFatal
	case "error":
		return logLevelError
	case "warn":
		return logLevelWarn
	case "info":
		return logLevelInfo
	case "verbose":
		return logLevelVerbose
	case "debug":
		return logLevelDebug
	case "trace":
		return logLevelTrace
	}
	panic("unexpected log level") // should never reach here
}

func Fatalf(format string, v ...interface{}) {
	log.Printlf(logLevelFatal, format, v...)
}

func Errorf(format string, v ...interface{}) {
	log.Printlf(logLevelError, format, v...)
}

func Warnf(format string, v ...interface{}) {
	log.Printlf(logLevelWarn, format, v...)
}

func Infof(format string, v ...interface{}) {
	log.Printlf(logLevelInfo, format, v...)
}

func Verbosef(format string, v ...interface{}) {
	log.Printlf(logLevelVerbose, format, v...)
}

func Debugf(format string, v ...interface{}) {
	log.Printlf(logLevelDebug, format, v...)
}

func Tracef(format string, v ...interface{}) {
	log.Printlf(logLevelTrace, format, v...)
}
