package main

import (
	"math/rand"
	"time"
)


func randomTimeout(minVal int, maxVal int) <-chan time.Time {
	randVal := minVal + rand.Intn(maxVal - minVal)
	randTime := time.Duration(randVal) * time.Millisecond
	return time.After(randTime)
}
