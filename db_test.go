package main

import (
	"errors"
	"fmt"
	"sync"
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
	db.Close()
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

func TestConcurrency(t *testing.T) {
	db, err := NewDB[TestVal]("concurrencyTest"+GenerateRandomKey(), "")
	if err != nil {
		panic(err)
	}
	startTime_1 := time.Now()
	var wg sync.WaitGroup
	for i := 1; i <= MaxTestEntries; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			key := GenerateRandomKey() + GenerateRandomKey() + GenerateRandomKey()
			value := fmt.Sprintf("%d", goroutineID)
			entry := TestEntry(value, i, "")
			res := db.Create(key, entry)
			require.Equal(t, nil, res.err)

		}(i)
	}
	wg.Wait()
	timeTaken_1 := time.Since(startTime_1)
	db.Close()
	fmt.Printf("Time taken to perform %v concurrent operation to create entries is %s", MaxTestEntries, timeTaken_1)
}

func TestStressCapacity(t *testing.T) {
	db, err := NewDB[TestVal]("stressTest"+GenerateRandomKey(), "")
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	for i := 1; i <= MaxTestEntries; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			key := fmt.Sprintf("%d", i)
			value := fmt.Sprintf("%d", goroutineID)
			entry := TestEntry(value, i, "")
			res := db.Create(key, entry)
			require.Equal(t, nil, res.err)

		}(i)
	}
	wg.Wait()
	for i := 1; i <= MaxTestEntries; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			key := fmt.Sprintf("%d", i)
			if i%2 != 0 {
				res := db.Delete(key)
				require.Equal(t, nil, res.err)
			}
		}(i)
	}
	wg.Wait()
	db.Close()
	t.Logf("Totally %v go routines has been used", 2*MaxTestEntries)
}
