package main

import (
	"encoding/json"
	"errors"
	"fmt" // Adjust the import path based on your setup
	"strconv"
	"sync"
	"time"
)

// A batch limit of 100-500 entries ensures efficient performance without overloading the system.
// This range strikes a balance between throughput and manageable data size (1.6 MB to 8 MB), as large batch sizes are uncommon in typical use cases.
// 100 entries * 16 KB = 1.6 MB
// 500 entries * 16 KB = 8 MB
const BatchLimit int = 500

const EntrySizeLimitMB = 16

const StorageLimitMB = 1024

const cleanpInterval = 2 * time.Second

type operationResult[T any] struct {
	err   error
	value DbData[T]
}
type operation[T any] struct {
	action    string
	key       string
	value     DbData[T]
	batchData map[string]DbData[T]
	response  chan operationResult[T]
}
type DB[T any] struct {
	localStorage  *LocalStorage[T]
	data          map[string]DbData[T]
	writeOps      chan operation[T]
	readOps       chan operation[T]
	rwLock        sync.RWMutex
	wg            sync.WaitGroup // To track ongoing operations
	closed        bool           // To signal when DB is closing
	closeCh       chan struct{}  // To signal all goroutines to stop
	stopCleanupCh chan struct{}  // Signal to stop the cleanup workercleann
}

func NewDB[T any](fileName string, dir string) (*DB[T], error) {
	loadedData := make(map[string]DbData[T])
	localStorage, err := NewLocalStorage(fileName, dir, &loadedData)
	if err != nil {
		return nil, err
	}
	db := &DB[T]{
		data:          loadedData,
		localStorage:  localStorage,
		writeOps:      make(chan operation[T], 100),
		readOps:       make(chan operation[T], 100),
		closeCh:       make(chan struct{}),
		closed:        false,
		stopCleanupCh: make(chan struct{}),
	}

	go db.writeWorker()
	go db.readWorker()
	go db.startCleanupWorker()

	return db, nil
}

func (db *DB[T]) Create(key string, value DbData[T]) operationResult[T] {
	db.rwLock.RLock()
	if db.closed {
		db.rwLock.RUnlock()
		println("DB Closed bro")
		return operationResult[T]{err: errors.New("DATABASE ALREADY CLOSED")}
	}
	db.rwLock.RUnlock()
	op := operation[T]{
		action:   "create",
		key:      key,
		value:    value,
		response: make(chan operationResult[T], 1),
	}
	db.writeOps <- op
	return <-op.response
}

func (db *DB[T]) Read(key string) operationResult[T] {
	db.rwLock.RLock()
	if db.closed {
		db.rwLock.RUnlock()
		return operationResult[T]{err: fmt.Errorf("DATABASE ALREADY CLOSED")}
	}
	db.rwLock.RUnlock()
	op := operation[T]{
		action:   "read",
		key:      key,
		response: make(chan operationResult[T], 1),
	}

	db.readOps <- op
	return <-op.response
}

func (db *DB[T]) BatchCreate(batchData map[string]DbData[T]) operationResult[T] {
	db.rwLock.RLock()
	if db.closed {
		db.rwLock.RUnlock()
		return operationResult[T]{err: fmt.Errorf("DATABASE ALREADY CLOSED")}
	}
	db.rwLock.RUnlock()
	op := operation[T]{
		action:    "batchCreate",
		batchData: batchData,
		response:  make(chan operationResult[T], 1),
	}

	db.writeOps <- op
	return <-op.response
}

func (db *DB[T]) writeWorker() {
	db.wg.Add(1)
	defer db.wg.Done()
	for op := range db.writeOps {
		var result operationResult[T]
		db.rwLock.Lock()

		switch op.action {
		case "create":
			err := db.create(op.key, op.value)
			result = operationResult[T]{err: err}

		case "batchCreate":
			err := db.batchCreate(op.batchData)
			result = operationResult[T]{err: err}

		case "delete":
			_, err := db.delete(op.key)
			result = operationResult[T]{err: err}

		default:
			err := fmt.Errorf("UNKNOWN OPERATION: %s", op.action)
			result = operationResult[T]{err: err}
		}

		db.rwLock.Unlock()
		op.response <- result
		close(op.response)
	}
}

func (db *DB[T]) readWorker() {
	db.wg.Add(1)
	defer db.wg.Done()
	for op := range db.readOps {
		var result operationResult[T]

		db.rwLock.RLock()
		switch op.action {
		case "read":
			value, err := db.read(op.key)
			result = operationResult[T]{err: err, value: value}
		default:
			err := fmt.Errorf("UNKNOWN OPERATION: %s", op.action)
			result = operationResult[T]{err: err}
		}
		db.rwLock.RUnlock()

		op.response <- result
		close(op.response)
	}
}

func (db *DB[T]) create(key string, value DbData[T]) error {
	if len(key) > 32 {
		return errors.New("KEY EXCEEDS THE MAXIMUM LENGTH OF 32 CHARACTERS")
	}

	if _, exists := db.data[key]; exists {
		return errors.New("ENTRY ALREADY EXISTS")
	}
	valueSize, valErr := db.isValidJson(db.data[key])
	if valErr != nil {
		return valErr
	}
	isSpaceAvailable, _, spaceErr := db.checkAvailableSpace(valueSize)
	if spaceErr != nil {
		return spaceErr
	}
	if !isSpaceAvailable {
		return errors.New("NOT AVAIABLE SPACE TO MAKE THE ENTRY")
	}
	db.data[key] = value
	err := db.localStorage.Sync(db.data)
	if err != nil {
		println("---------------Rollback---------------------")
		delete(db.data, key)
		return err
	}

	return nil
}

func (db *DB[T]) batchCreate(batchData map[string]DbData[T]) error {
	// A batch limit of 100-500 entries ensures efficient performance without overloading the system.
	// This range strikes a balance between throughput and manageable data size (1.6 MB to 8 MB), as large batch sizes are uncommon in typical use cases.
	// 100 entries * 16 KB = 1.6 MB
	// 500 entries * 16 KB = 8 MB
	if len(batchData) > BatchLimit {
		return errors.New("BATCHDATA IS LARGE")
	}
	for key := range batchData {
		if len(key) > 32 {
			return errors.New("KEY EXCEEDS THE MAXIMUM LENGTH OF 32 CHARACTERS")
		}

		if _, exists := db.data[key]; exists {
			return fmt.Errorf("ENTRY ALREADY EXISTS %s", key)
		}
		_, valErr := db.isValidJson(batchData[key])
		if valErr != nil {
			return valErr
		}
	}
	jsonBatchedData, jsonErr := json.Marshal(batchData)
	if jsonErr != nil {
		return jsonErr
	}
	jsonBatchedDataSizeKb := BytesToKB(len(jsonBatchedData))
	isSpaceAvailable, fileSize, spaceErr := db.checkAvailableSpace(jsonBatchedDataSizeKb)
	if spaceErr != nil {
		return spaceErr
	}
	if !isSpaceAvailable {
		return fmt.Errorf("Batch Size exceeds storage limit , Batch Operation Size = %.2f mb  , Avaiable Storage Space = %.2fmb", kbToMb(jsonBatchedDataSizeKb), StorageLimitMB-kbToMb(fileSize))
	}
	// fmt.Printf("Batch Operation :%.2f mb, %.2f\n", kbToMb(jsonBatchedDataSizeKb), jsonBatchedDataSizeKb)
	for key, value := range batchData {
		db.data[key] = value
	}
	err := db.localStorage.Sync(db.data)
	if err != nil {
		for key := range batchData { // rollback
			delete(db.data, key)
		}
		return err
	}
	// val, _ := db.localStorage.getFileSizeInKB()
	// fmt.Printf("After writing file size :%.2f mb", kbToMb(val))
	return nil
}

func (db *DB[T]) Delete(key string) operationResult[T] {
	db.rwLock.RLock()
	if db.closed {
		db.rwLock.RUnlock()
		return operationResult[T]{err: fmt.Errorf("DATABASE ALREADY CLOSED")}
	}
	db.rwLock.RUnlock()
	op := operation[T]{
		action:   "delete",
		key:      key,
		response: make(chan operationResult[T], 1),
	}

	db.writeOps <- op
	return <-op.response
}

func (db *DB[T]) delete(key string) (bool, error) {
	if _, exists := db.data[key]; exists {
		isExpired := db.IsExpired(key)
		if isExpired {
			return false, errors.New("ENTRY EXPIRED")
		}
		delete(db.data, key)
		err := db.localStorage.Sync(db.data)
		if err != nil {
			db.localStorage.Load(&db.data) // rollback , reloading file in to memory
			return false, err
		}

		return true, nil
	}
	return false, errors.New("KEY NOT FOUND")
}

func (db *DB[T]) read(key string) (DbData[T], error) {

	if valueObj, exists := db.data[key]; exists {
		if db.IsExpired(key) {
			return DbData[T]{}, errors.New("ENTRY EXPIRED")
		}
		return valueObj, nil
	}
	return DbData[T]{}, errors.New("KEY NOT FOUND")
}
func (db *DB[T]) IsExpired(key string) bool {
	if db.data[key].Ttl == "" {
		return false
	}
	seconds, err := strconv.Atoi(db.data[key].Ttl)
	if err != nil {
		return false // todo : handle
	}
	return time.Now().After(db.data[key].Created_at.Add(time.Duration(seconds) * time.Second))
}

func (db *DB[T]) PrintValue(key string) {
	data := db.data[key]
	fmt.Printf("DbData:\n  Value: %v\n  Ttl: %v\n  Created_at: %v\n", data.Value, data.Ttl, data.Created_at)
}

func (db *DB[T]) isValidJson(data DbData[T]) (float64, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return 0, fmt.Errorf("FAILED TO CONVERT MAP TO JSON: %w", err)
	}
	// fmt.Printf("Size in kilobytes: %d\n", len(jsonData))
	// fmt.Printf("Size in kilobytes: %.2f KB\n", BytesToKB(len(jsonData)))
	// fmt.Printf("Entry Value: %+v\n", data)
	// fmt.Printf("Size in kilobytes: %.2f KB\n", BytesToKB(len(jsonData)))
	jsonSize := BytesToKB(len(jsonData))
	if jsonSize > EntrySizeLimitMB*1024 {
		return jsonSize, fmt.Errorf("JSON size exceeds the 16 KB limit")
	}
	return jsonSize, nil
}
func (db *DB[T]) checkAvailableSpace(entrySizeKB float64) (bool, float64, error) {
	FileSizekB, err := db.localStorage.getFileSizeInKB()
	if err != nil {
		return false, 0, fmt.Errorf("FAILED TO GET CURRENT FILE SIZE: %w", err)
	}
	// fmt.Printf("File Size Current :%.2f mb\n", kbToMb(FileSizekB))
	if FileSizekB+entrySizeKB > StorageLimitMB*1024 {
		return false, FileSizekB, nil
	}

	return true, FileSizekB, nil
}
func (db *DB[T]) Close() error {
	db.rwLock.Lock()

	if db.closed {
		db.rwLock.Unlock()
		return errors.New("DB is already closed")
	}

	db.closed = true
	db.rwLock.Unlock()

	close(db.stopCleanupCh)

	// Close channels - any existing operations in the channels
	// will still be processed.
	close(db.writeOps)
	close(db.readOps)

	db.wg.Wait()

	return db.localStorage.releaseLock()
}

func (db *DB[T]) startCleanupWorker() {
	db.wg.Add(1)
	defer db.wg.Done()

	ticker := time.NewTicker(cleanpInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			db.cleanupExpiredKeys()
		case <-db.stopCleanupCh:
			return
		}
	}
}

func (db *DB[T]) cleanupExpiredKeys() {
	db.rwLock.Lock()
	defer db.rwLock.Unlock()
	for key := range db.data {
		if db.IsExpired(key) {
			delete(db.data, key)
		}
	}
	db.localStorage.Sync(db.data)
}
