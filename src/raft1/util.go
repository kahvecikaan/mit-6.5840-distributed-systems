package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func randomDurationBetween(min, max time.Duration) time.Duration {
	minNano := min.Nanoseconds()
	maxNano := max.Nanoseconds()
	delta := maxNano - minNano + 1
	randomNano := rand.Int63n(delta) // [0, delta)

	return time.Duration(minNano + randomNano) // [min,max]
}
