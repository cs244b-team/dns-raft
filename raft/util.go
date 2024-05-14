package raft

import (
	"math/rand"
	"time"
)

type RaftTimer struct {
	timeout time.Duration
	c       <-chan time.Time
}

func NewRandomTimer(minVal int, maxVal int) *RaftTimer {
	randVal := minVal + rand.Intn(maxVal-minVal)
	randTime := time.Duration(randVal) * time.Millisecond
	return &RaftTimer{randTime, time.After(randTime)}
}
