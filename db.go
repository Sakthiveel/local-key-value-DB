package main

import (
	"encoding/json"
	"fmt" // Adjust the import path based on your setup
	"local-key-value-DB/dbError"
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

const cleanpInterval = time.Minute

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
	mu            sync.Mutex             // Protects access to the locks map
	locks         map[string]*sync.Mutex // Per-key locks
	wg            sync.WaitGroup         // To track ongoing operations
	closed        bool                   // To signal when DB is closing
	closeCh       chan struct{}          // To signal all goroutines to stop
	stopCleanupCh chan struct{}          // Signal to stop the cleanup workercleann
}

func NewDB[T any](fileName string, dir string) (*DB[T], error) {
	loadedData := make(map[string]DbData[T])
	localStorage, err := NewLocalStorage(fileName, dir, &loadedData)
	if err != nil {
		return nil, err
	}
	db := &DB[T]{
		localStorage:  localStorage,
		data:          loadedData,
		writeOps:      make(chan operation[T], 100),
		readOps:       make(chan operation[T], 100),
		locks:         make(map[string]*sync.Mutex),
		closeCh:       make(chan struct{}),
		stopCleanupCh: make(chan struct{}),
		closed:        false,
	}

	go db.writeWorker()
	go db.readWorker()
	go db.startCleanupWorker()

	return db, nil
}

func (db *DB[T]) getLock(key string) *sync.Mutex {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, exists := db.locks[key]; !exists {
		db.locks[key] = &sync.Mutex{}
	}
	return db.locks[key]
}

func (db *DB[T]) Create(key string, value DbData[T]) operationResult[T] {
	if db.closed {
		return operationResult[T]{err: dbError.DBAlreadyClosed("")}
	}
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
	if db.closed {
		return operationResult[T]{err: dbError.DBAlreadyClosed("")}
	}
	op := operation[T]{
		action:   "read",
		key:      key,
		response: make(chan operationResult[T], 1),
	}

	db.readOps <- op
	return <-op.response
}

func (db *DB[T]) BatchCreate(batchData map[string]DbData[T]) operationResult[T] {
	if db.closed {
		return operationResult[T]{err: dbError.DBAlreadyClosed("")}
	}
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
		entryLock := db.getLock(op.key)
		entryLock.Lock()

		switch op.action {
		case "create":
			err := db.create(op.key, op.value)
			result = operationResult[T]{err: err}
		case "batchCreate":
			err := db.batchCreate(op.batchData)
			result = operationResult[T]{err: err}
		case "delete":
			err := db.delete(op.key)
			result = operationResult[T]{err: err}
		case "update":
			err := db.update(op.key, op.value)
			result = operationResult[T]{err: err}
		default:
			err := dbError.UnkownOperation(op.action)
			result = operationResult[T]{err: err}
		}
		entryLock.Unlock()
		op.response <- result
		close(op.response)
	}
}

func (db *DB[T]) readWorker() {
	db.wg.Add(1)
	defer db.wg.Done()
	for op := range db.readOps {
		var result operationResult[T]
		entryLock := db.getLock(op.key)
		entryLock.Lock()
		switch op.action {
		case "read":
			value, err := db.read(op.key)
			result = operationResult[T]{err: err, value: value}
		default:
			err := dbError.UnkownOperation(op.action)
			result = operationResult[T]{err: err}
		}
		entryLock.Unlock()
		op.response <- result
		close(op.response)
	}
}

func (db *DB[T]) create(key string, value DbData[T]) error {
	entrySize, entryErr := db.isEntryValid(key, value)
	if entryErr != nil {
		return entryErr
	}
	isSpaceAvailable, _, spaceErr := db.checkAvailableSpace(entrySize)
	if spaceErr != nil {
		return spaceErr
	}
	if !isSpaceAvailable {
		return dbError.NotAvailabeSpace("")
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
		return dbError.BatchLimitCountExceeds("")
	}
	for key := range batchData {
		_, entryErr := db.isEntryValid(key, batchData[key])
		if entryErr != nil {
			return entryErr
		}
	}
	jsonBatchedData, jsonErr := json.Marshal(batchData)
	if jsonErr != nil {
		return jsonErr
	}
	jsonBatchedDataSizeKb := BytesToKB(len(jsonBatchedData))
	isSpaceAvailable, _, spaceErr := db.checkAvailableSpace(jsonBatchedDataSizeKb)
	if spaceErr != nil {
		return spaceErr
	}
	if !isSpaceAvailable {
		return dbError.BatchSizeLimitCrossed("")
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
	if db.closed {
		return operationResult[T]{err: dbError.DatabaseAlreadyClose("")}
	}
	op := operation[T]{
		action:   "delete",
		key:      key,
		response: make(chan operationResult[T], 1),
	}

	db.writeOps <- op
	return <-op.response
}

func (db *DB[T]) delete(key string) error {
	if _, exists := db.data[key]; exists {
		isExpired := db.IsExpired(key)
		err := db.deleteEntry(key)
		if err != nil && !isExpired {
			return err
		} else if isExpired {
			return dbError.KeyExpired("")
		}
		return err
	}
	return dbError.KeyNotFound("")
}

func (db *DB[T]) read(key string) (DbData[T], error) {

	if valueObj, exists := db.data[key]; exists {
		if db.IsExpired(key) {
			db.deleteEntry(key)
			return DbData[T]{}, dbError.KeyExpired("")
		}
		return valueObj, nil
	}
	return DbData[T]{}, dbError.KeyNotFound("")
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
		return 0, dbError.FailedToConvertMapToJson(fmt.Sprintf("%s", err))
	}
	// fmt.Printf("Size in kilobytes: %d\n", len(jsonData))
	// fmt.Printf("Size in kilobytes: %.2f KB\n", BytesToKB(len(jsonData)))
	// fmt.Printf("Entry Value: %+v\n", data)
	// fmt.Printf("Size in kilobytes: %.2f KB\n", BytesToKB(len(jsonData)))
	jsonSize := BytesToKB(len(jsonData))
	if jsonSize > EntrySizeLimitMB*KB {
		return jsonSize, dbError.JsonSizeExceedsLimit("")
	}
	return jsonSize, nil
}
func (db *DB[T]) checkAvailableSpace(entrySizeKB float64) (bool, float64, error) {
	FileSizekB, err := db.localStorage.getFileSizeInKB()
	if err != nil {
		return false, 0, dbError.FailedToGetFileSize("")
	}
	// fmt.Printf("File Size Current :%.2f mb\n", kbToMb(FileSizekB))
	if FileSizekB+entrySizeKB > StorageLimitMB*KB {
		return false, FileSizekB, nil
	}

	return true, FileSizekB, nil
}
func (db *DB[T]) Close() error {

	if db.closed {
		return dbError.DBAlreadyClosed("")
	}

	db.closed = true

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
	for key := range db.data {
		if db.IsExpired(key) {
			entryLock := db.getLock(key)
			entryLock.Lock()
			defer entryLock.Unlock()
			delete(db.data, key)
		}
	}
	db.localStorage.Sync(db.data)
}
func (db *DB[T]) deleteEntry(key string) error {
	entry := db.data[key]
	delete(db.data, key)
	err := db.localStorage.Sync(db.data)
	if err != nil {
		// rollback
		db.data[key] = entry
	}
	return err
}
func (db *DB[T]) isEntryValid(key string, value DbData[T]) (float64, error) {
	if len(key) > 32 {
		return 0, dbError.KeySizeExceedsLimit(32, "")
	}
	if _, exists := db.data[key]; exists {
		if db.IsExpired(key) {
			db.deleteEntry(key) // no need to pass the error (will get roll back)
		}
		return 0, dbError.EntryAlreadyExists(fmt.Sprintf("key : %s", key))
	}
	valueSize, valErr := db.isValidJson(value)
	if valErr != nil {
		return 0, valErr
	}
	return valueSize, nil
}
func (db *DB[T]) Update(key string, value DbData[T]) operationResult[T] {
	if db.closed {
		return operationResult[T]{err: dbError.DBAlreadyClosed("")}
	}
	op := operation[T]{
		action:   "update",
		key:      key,
		value:    value,
		response: make(chan operationResult[T], 1),
	}
	db.writeOps <- op
	return <-op.response
}

func (db *DB[T]) update(key string, updatedVal DbData[T]) error {
	_, entryExists := db.data[key]
	if !entryExists {
		return dbError.EntryNotExists("")
	}
	if db.IsExpired(key) {
		delete(db.data, key)
		return dbError.EntryExpired("")
	}
	entrySize, _ := db.isEntryValid(key, updatedVal)
	// TODO: handle entryErr here
	// if entryErr != nil && !errors.As(entryErr, dbError.EntryAlreadyExists("").Error()) {
	// 	return entryErr
	// }
	isSpaceAvailable, _, spaceErr := db.checkAvailableSpace(entrySize)
	if spaceErr != nil {
		return spaceErr
	}
	if !isSpaceAvailable {
		return dbError.NotAvailabeSpace("")
	}
	previousVal := db.data[key]
	db.data[key] = updatedVal
	err := db.localStorage.Sync(db.data)
	if err != nil {
		println("---------------Rollback---------------------")
		db.data[key] = previousVal
		return err
	}

	return nil
}
