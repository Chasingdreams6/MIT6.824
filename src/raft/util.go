package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dError logTopic = "ERRO"
	dInfo  logTopic = "INFO"
	dWarn  logTopic = "WARN"
	dHERT  logTopic = "LOG1"
	dRole  logTopic = "LOG2"
	dREPL  logTopic = "CLNT"
	dAPPL  logTopic = "APPL"
	dCMIT  logTopic = "CMIT"
	dEntr  logTopic = "ENTR"
	dSIZE  logTopic = "SIZE"
	dWLOG  logTopic = "WLOG"
	dRLOG  logTopic = "RLOG"
	dLOCK  logTopic = "RLCK"
	dULCK  logTopic = "ULCK"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	log.SetFlags(log.Flags() & ^(log.Ldate | log.Ltime))
}

func DebugOutput(topic logTopic, format string, a ...interface{}) {
	//if topic == dLOCK || topic == dULCK {
	//	if debugVerbosity >= 2 {
	//		time := time.Since(debugStart).Microseconds()
	//		time /= 100
	//		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
	//		format = prefix + format
	//		log.Printf(format, a...)
	//	}
	//	return
	//}
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}

}

func DebugExample() {
	DebugOutput(dInfo, "S%d Leader, checking hb", 1)
}
