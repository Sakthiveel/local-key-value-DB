package main

import (
	"local-key-value-DB/dbError"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

const (
	KB = 1024
	MB = KB * 1024
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
	return float64(sizeInBytes) / float64(KB)
}

func kbToMb(sizeInKb float64) float64 {
	return sizeInKb / float64(KB)
}

func ValidateAndFixJSONFilename(filename string) (string, error) {
	filename = strings.TrimSpace(filename)

	if len(filename) == 0 {
		return "default_file.json", nil
	} else if len(filename) > 24 {
		return "", dbError.InvalidFileName("file name exceeds max limit of 24 characters")
	}

	invalidCharPattern := regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)
	if invalidCharPattern.MatchString(filename) {
		return "", dbError.InvalidFileName("contains invalid characters")
	}

	// todo: need to check  more os specific reserved keywords
	reservedNames := []string{"con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9"}
	baseName := strings.ToLower(strings.TrimSuffix(filename, filepath.Ext(filename)))
	for _, reserved := range reservedNames {
		if baseName == reserved {
			return "", dbError.InvalidFileName("fileName is a reserved name")
		}
	}

	ext := filepath.Ext(filename)
	if ext == ".json" {
		nameWithoutExt := strings.TrimSuffix(filename, ext)
		if strings.Contains(nameWithoutExt, ".") {
			return "", dbError.InvalidFileName("contains extra dot")
		}
		return filename, nil
	}

	if ext != "" && ext != ".json" {
		return "", dbError.InvalidFileName("no file extension or wrong extension.")
	}

	// todo: this check is necessary ?
	if strings.Contains(filename, ".") {
		return "", dbError.InvalidFileName("contains extra dot")
	}

	return filename + ".json", nil
}
