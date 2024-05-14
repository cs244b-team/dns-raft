package main

import (
	"math/rand"
	"time"
)

type RaftTimer struct {
	MinVal int
	MaxVal int
	inner  *time.Timer
}

func NewTimer(minVal int, maxVal int) RaftTimer {
	return RaftTimer{MinVal: minVal, MaxVal: maxVal, inner: nil}
}

func (timer *RaftTimer) Start() <-chan time.Time {
	randVal := timer.MinVal + rand.Intn(timer.MaxVal-timer.MinVal)
	randTime := time.Duration(randVal) * time.Millisecond
	if timer.inner == nil {
		timer.inner = time.NewTimer(randTime)
	} else {
		timer.inner.Reset(randTime)
	}
	return timer.inner.C
}

func (timer *RaftTimer) Stop() {
	if !timer.inner.Stop() {
		<-timer.inner.C
	}
}
