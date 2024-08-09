package mr

import (
	"fmt"
	"log"
)

type Logger struct {
	prefix string
}

func NewLogger(prefix string) *Logger {
	return &Logger{
		prefix: prefix,
	}
}

func (l *Logger) Println(v ...interface{}) {
	log.Println(l.prefix + " " + fmt.Sprint(v...))
}

func (l *Logger) Printf(format string, v ...interface{}) {
	log.Printf(l.prefix+" "+format, v...)
}

func (l *Logger) Fatal(v ...interface{}) {
	log.Fatal(l.prefix + " " + fmt.Sprint(v...))
}

func (l *Logger) Fatalf(format string, v ...interface{}) {
	log.Fatalf(l.prefix+" "+format, v...)
}
