package mr

import (
	"fmt"
	"log"
	"os"
)

type Logger struct {
	prefix string
	ilog   log.Logger
}

func NewLogger(prefix string) *Logger {
	ret := Logger{
		prefix: prefix,
		ilog:   log.Logger{},
	}
	// set here to io.Discard os.Stderr to disable or enable log
	// ret.ilog.SetOutput(io.Discard)
	ret.ilog.SetOutput(os.Stderr)

	ret.ilog.SetFlags(log.LstdFlags)
	return &ret
}

func (l *Logger) Println(v ...interface{}) {
	l.ilog.Println(l.prefix + " " + fmt.Sprint(v...))
}

func (l *Logger) Printf(format string, v ...interface{}) {
	l.ilog.Printf(l.prefix+" "+format, v...)
}

func (l *Logger) Fatal(v ...interface{}) {
	l.ilog.Fatal(l.prefix + " " + fmt.Sprint(v...))
}

func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.ilog.Fatalf(l.prefix+" "+format, v...)
}
