package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) sample() {
	rf.mu.Lock()
	go rf.timeUpdate()
	rf.mu.Unlock()
}
