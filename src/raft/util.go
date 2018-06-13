package raft

import "log"

// Debugging

const (
	DEBUG = iota
	INFO
	WARN
	ERROR
)
const LEVEL = ERROR

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if LEVEL <= DEBUG  {
		log.Printf(format, a...)
	}
	return
}

func IPrintf(format string, a ...interface{}) (n int, err error) {
	if LEVEL <= INFO  {
		log.Printf(format, a...)
	}
	return
}

func WPrintf(format string, a ...interface{}) (n int, err error) {
	if LEVEL <= WARN {
		log.Printf(format, a...)
	}
	return
}

func EPrintf(format string, a ...interface{}) (n int, err error) {
	if LEVEL <= ERROR {
		log.Printf(format, a...)
	}
	return
}