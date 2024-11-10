package main

import (
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

type DbData[T any] struct {
	Value      T         `json:"value"`
	Ttl        string    `json:"ttl"` // if string empty means no expiration time
	Created_at time.Time `json:"created_at"`
}

func NewDbData[T any](value T, ttlSeconds string) DbData[T] {
	return DbData[T]{
		Value:      value,
		Ttl:        ttlSeconds,
		Created_at: time.Now(),
	}
}

type TestVal struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func NewTestVal(name string, age int) TestVal {
	return TestVal{
		Name: name,
		Age:  age,
	}
}

func TestEntry(name string, age int, ttlSeconds string) DbData[TestVal] {
	return NewDbData[TestVal](NewTestVal(name, age), ttlSeconds)
}

func IsAlphanumeric(str string) bool {
	if str == "" {
		return false
	}
	matched, _ := regexp.MatchString(`^[a-zA-Z0-9]+$`, str)
	return matched
}

var sequenceCounter uint32

// todo:
func GenerateRandomKey() string {
	timestamp := uint32(time.Now().UnixNano())

	counter := atomic.AddUint32(&sequenceCounter, 1)

	mixed := timestamp ^ (counter * 2654435761) // multiply by prime number for better distribution

	var result strings.Builder
	result.Grow(5) // Pre-allocate space for 5 digits

	for i := 0; i < 5; i++ {
		mixed = (mixed * 1664525) + 1013904223 // Linear congruential generator
		digit := mixed % 10
		result.WriteByte(byte(digit) + '0')
	}

	return result.String()
}

func BytesToKB(sizeInBytes int) float64 {
	return float64(sizeInBytes) / 1024.0
}

func kbToMb(sizeInKb float64) float64 {
	return sizeInKb / 1024.0
}
