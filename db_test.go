package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var MaxTestEntries = 500

func TestFileNameValidation(t *testing.T) {
	testFiles := []string{
		"image.jpg",
		"file.",
		".gitignore",
		"folder/file",
		"test.go",
	}

	for _, file := range testFiles {
		_, err := NewDB[TestVal](file, "")
		require.ErrorContains(t, err, "INVALID FILE NAME")
	}
}

func TestStoreInit(t *testing.T) {
	dbIns, err := NewDB[TestVal]("TestStoreInit"+GenerateRandomKey(), "")
	if err != nil {
		panic(err)
	}
	key := "storeInit" + GenerateRandomKey()
	entry := TestEntry("value here,", 12, "")
	dbIns.Create(key, entry)

	res := dbIns.Read(key)

	require.Equal(t, nil, res.err)
	require.Equal(t, entry.Value, res.value.Value)
	dbIns.Close()
}

func TestAllowOnlyOneClientConnection(t *testing.T) {
	fileName := "allowOneConnection" + GenerateRandomKey()
	dbIns_1, err_1 := NewDB[TestVal](fileName, "")
	if err_1 != nil {
		panic(err_1)
	}
	key := "key-1" + GenerateRandomKey()
	_, err_2 := NewDB[TestVal](fileName, "")
	require.ErrorContains(t, err_2, "FAILED TO ACQUIRE LOCK")
	dbIns_1.Close()

	dbsIns_3, err_3 := NewDB[TestVal](fileName, "")
	require.Equal(t, nil, err_3)
	entry_3 := TestEntry("value 1", 43, "")
	dbsIns_3.create(key, entry_3)

	res := dbsIns_3.Read(key)

	require.Equal(t, nil, res.err)
	require.Equal(t, entry_3, res.value)
	dbsIns_3.Close()
}

func TestBasicCrdOperation(t *testing.T) {
	db, err := NewDB[TestVal]("basicCRD"+GenerateRandomKey(), "")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	key_1 := "one" + GenerateRandomKey()
	key_2 := "two" + GenerateRandomKey()
	entry_1 := TestEntry("value_1", 12, "")
	db.Create(key_1, entry_1)

	entry_2 := TestEntry("value_2", 34, "")
	db.Create(key_2, entry_2)
	readRes := db.Read(key_1)
	require.Equal(t, entry_1.Value, readRes.value.Value)

	res := db.Create(key_1, entry_1)
	require.Equal(t, errors.New("ENTRY ALREADY EXISTS"), res.err)

	delRes := db.Delete(key_1)
	require.Equal(t, nil, delRes.err)
}

func TestTTLChecking(t *testing.T) {
	db, err := NewDB[TestVal]("testTTL"+GenerateRandomKey(), "")
	if err != nil {
		panic(err)
	}
	key := "ttl" + GenerateRandomKey()
	entry := TestEntry("value here", 34, "5")
	db.create(key, entry)
	time.Sleep(2 * time.Second)
	res_1 := db.Read(key)
	require.Equal(t, entry, res_1.value)
	res_2 := db.Read(key)
	require.Equal(t, entry, res_2.value)
	time.Sleep(3 * time.Second)
	res_3 := db.Read(key)
	require.Equal(t, res_3.err, errors.New("ENTRY EXPIRED"))
	db.Close()
}

func TestBatchCreation(t *testing.T) {
	db, err := NewDB[TestVal]("batchCreation"+GenerateRandomKey(), "")
	if err != nil {
		panic(err)
	}
	dataMap := make(map[string]DbData[TestVal])
	for i := 1; i <= MaxTestEntries; i++ {
		key := GenerateRandomKey() + GenerateRandomKey() + GenerateRandomKey()
		value := fmt.Sprintf("Value %d", i)
		dataMap[key] = TestEntry(value, i, "")
	}
	startTime := time.Now()
	res := db.BatchCreate(dataMap)
	duration := time.Now().Sub(startTime).Seconds()
	println("-----------------------------------------------", duration)
	require.Equal(t, nil, res.err)
	db.Close()
}

func TestNotOverwriting(t *testing.T) {
	db, err := NewDB[TestVal]("testotoverwrite"+GenerateRandomKey(), "")
	if err != nil {
		panic(err)
	}
	key := "key-overwrite" + GenerateRandomKey()
	entry_1 := TestEntry("sample value", 34, "")
	db.create(key, entry_1)
	readRes := db.Read(key)
	require.Equal(t, entry_1, readRes.value)

	res := db.create(key, entry_1)

	require.ErrorContains(t, res, "ENTRY ALREADY EXISTS")

	db.Close()
}

func TestLoadExistinFile(t *testing.T) {
	fileName := "loadExist" + GenerateRandomKey()
	dbIns_1, err_1 := NewDB[TestVal](fileName, "")
	if err_1 != nil {
		panic(err_1)
	}
	key := "load1" + GenerateRandomKey()
	entry := TestEntry("load value", 12, "")
	dbIns_1.create(key, entry)
	dbIns_1.Close()

	dbIns_2, err_2 := NewDB[TestVal](fileName, "")

	if err_2 != nil {
		panic(err_2)
	}
	res := dbIns_2.Read(key)
	require.Equal(t, nil, res.err)
	dbIns_2.PrintValue(key)
	require.Equal(t, entry.Value, res.value.Value)
	dbIns_2.Close()
}

func TestConcurrentCreateRead(t *testing.T) {
	db, err := NewDB[TestVal]("testdata"+GenerateRandomKey(), "")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer db.Close()

	// Test data
	testKey := "test_key"

	// Number of concurrent operations
	numOps := 500

	// Number of entries to create before concurrency
	n := 100

	// WaitGroup to ensure all goroutines finish
	var wg sync.WaitGroup

	// Create n entries before starting the concurrent phase
	for i := 0; i < numOps; i++ {
		key := testKey + strconv.Itoa(i)
		entry := TestEntry("person_"+strconv.Itoa(i), i, "")
		result := db.Create(key, entry)
		if result.err != nil {
			t.Fatalf("Pre-concurrency Create failed for key %s: %v", key, result.err)
		}
	}

	// Barrier to ensure all goroutines start at the same time
	startBarrier := make(chan struct{})

	// Measure time for Create and Read operations concurrently
	startCreateRead := time.Now()

	// Launch Create goroutines for the concurrent phase
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			key := "new_key" + strconv.Itoa(n+i) // Ensure keys don't overlap with pre-created entries
			entry := TestEntry("person_"+strconv.Itoa(n+i), n+i, "")

			// Wait for the start signal
			<-startBarrier

			// Perform Create operation
			result := db.Create(key, entry)
			if result.err != nil {
				t.Errorf("Create failed for key %s: %v", key, result.err)
			}
		}(i)
	}

	// Launch Read goroutines for the concurrent phase
	for i := 0; i < numOps; i++ { // Read both pre-created and new entries
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			key := testKey + strconv.Itoa(i)

			// Wait for the start signal
			<-startBarrier

			// Perform Read operation
			result := db.Read(key)
			checkEntry := TestEntry("person_"+strconv.Itoa(i), i, "")
			require.Equal(t, checkEntry.Value, result.value.Value)
			// if result.err != nil && result.err.Error() != "KEY NOT FOUND" {
			// 	t.Errorf("Read failed for key %s: %v", key, result.err)
			// }
			// if result.err != nil {
			// 	require.ErrorContains(t, result.err, "KEY NOT FOUND")
			// } else {
			// 	require.Equal(t, checkEntry.Value, result.value.Value)
			// }
		}(i)
	}

	// Release all goroutines at the same time
	close(startBarrier)

	// Wait for all goroutines to finish
	wg.Wait()

	totalDuration := time.Since(startCreateRead)
	// throughput := float64(numOps*2) / totalDuration.Seconds() // both reads and writes during concurrency

	// fmt.Printf("Total Throughput: %.2f ops/sec\n", throughput)

	// Ensure all data is consistent after concurrency
	for i := 0; i < numOps; i++ { // Check all entries created both before and during concurrency
		checkEntry := TestEntry("person_"+strconv.Itoa(i), i, "")
		key := testKey + strconv.Itoa(i)
		result := db.Read(key)
		require.Equal(t, checkEntry.Value, result.value.Value)
	}

	require.Equal(t, len(db.data), numOps+numOps)
	fmt.Printf("The map size is %v\n", len(db.data))

	fmt.Printf("Total Time taken to run %v concurrent reads and writes: %s\n", numOps, totalDuration)
}

func TestDBClose(t *testing.T) {
	var wg sync.WaitGroup
	count := 5
	db, err := NewDB[string]("dbclose"+GenerateRandomKey(), "")
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	var successOps, failedOps, inProgressOps atomic.Int32
	var closeOnce sync.Once

	// Channel to signal operations that were queued before close
	operationsBeforeClose := make(chan struct{})

	for i := 1; i <= count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("%d", i)
			entry := "value_" + strconv.Itoa(i)

			if i == count/2 { // Close DB halfway through
				closeOnce.Do(func() {
					// Signal that operations before this point should complete
					close(operationsBeforeClose)
					// Small delay to ensure some operations are in-flight
					time.Sleep(10 * time.Millisecond)
					if err := db.Close(); err != nil {
						t.Errorf("Failed to close DB: %v", err)
					}
				})
			}

			// Mark operation as "in progress" before checking DB closed state
			select {
			case <-operationsBeforeClose:
				// Operation was queued before close signal
				inProgressOps.Add(1)
			default:
				// Operation attempted after close signal
			}

			// Perform Create operation
			result := db.Create(key, NewDbData(entry, ""))

			if result.err != nil {
				if strings.Contains(result.err.Error(), "DATABASE ALREADY CLOSED") {
					failedOps.Add(1)
				} else {
					t.Errorf("Unexpected error for key %s: %v", key, result.err)
				}
			} else {
				successOps.Add(1)
			}
		}(i)
	}

	wg.Wait()

	// Log detailed statistics
	t.Logf("Operations - Total: %d, Successful: %d, Failed: %d, In-Progress at Close: %d",
		count, successOps.Load(), failedOps.Load(), inProgressOps.Load())

	// Verify results
	if successOps.Load()+failedOps.Load() != int32(count) {
		t.Errorf("Expected %d total operations, got %d",
			count, successOps.Load()+failedOps.Load())
	}
	// Verify in-progress operations completed
	if successOps.Load() < inProgressOps.Load() {
		t.Errorf("Some in-progress operations failed: "+
			"In-progress: %d, Successful: %d",
			inProgressOps.Load(), successOps.Load())
	}

	// Verify some operations failed after close
	if failedOps.Load() == 0 {
		t.Error("Expected some operations to fail after close")
	}

	// Verify DB is fully closed
	finalResult := db.Read("1")
	require.ErrorContains(t, finalResult.err, "DATABASE ALREADY CLOSED")
}
