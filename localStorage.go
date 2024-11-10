package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

type LocalStorage[T any] struct {
	file     string // todo: change to fileName
	lockFile *os.File
}

func NewLocalStorage[T any](fileName string, dir string, dataToLoad *map[string]DbData[T]) (*LocalStorage[T], error) {
	if dir == "" {
		curDir, osErr := os.Getwd()
		if osErr != nil {
			return nil, osErr
		}
		dir = curDir
	}
	if fileName == "" {
		fileName = "default_file.json"
	}
	if !IsAlphanumeric(fileName) { // todo: temporary check , need to be improved , exclude os reserved keyword
		return nil, errors.New("INVALID FILE NAME")
	}
	filePath := filepath.Join(dir, fileName+".json")
	// println("filePaht", filePath)
	localStorage := &LocalStorage[T]{
		file: filePath,
	}

	fileExists, err := localStorage.fileExists(dir)
	if err != nil {
		return nil, err
	}
	if !fileExists {
		localStorage.createFile()
	} else {
		localStorage.Load(dataToLoad)
	}
	// Acquire lock immediately when creating database
	if err := localStorage.acquireLock(); err != nil {
		return nil, fmt.Errorf("Failed to acquire lock: %w", err)
	}
	// or else throw error
	return localStorage, nil
}

func (ls *LocalStorage[T]) createFile() error {
	dir := filepath.Dir(ls.file)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to check directory: %w", err)
	}

	// fmt.Println("Creating file at:", ls.file)
	file, err := os.Create(ls.file)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Initialize the file with an empty map
	return ls.Sync(make(map[string]DbData[T]))
}
func (ls *LocalStorage[T]) fileExists(dir string) (bool, error) {

	_, dirErr := os.Stat(dir)

	// fmt.Printf("Given Dir :%v\n", dir)
	// fmt.Printf("Dir Err :%v\n", dirErr)
	// fmt.Printf("Given FilePath :%v\n", ls.file)

	if os.IsNotExist(dirErr) {
		return false, fmt.Errorf("Directory not exist")
	}

	_, err := os.Stat(ls.file)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	// Handle any other type of error (e.g., permission errors)
	return false, fmt.Errorf("failed to check if file exists: %w", err)
}

func (ls *LocalStorage[T]) Sync(data map[string]DbData[T]) error {
	// fmt.Printf("Sync data %+v\n ", data)
	file, err := os.Create(ls.file)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	return encoder.Encode(data)
}

func (ls *LocalStorage[T]) Load(dataToLoad *map[string]DbData[T]) error {
	file, err := os.Open(ls.file)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	return decoder.Decode(&dataToLoad)
}

// acquireLock tries to get an exclusive lock on the file
func (ls *LocalStorage[T]) acquireLock() error {
	var err error
	ls.lockFile, err = os.OpenFile(ls.file+".lock", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	err = syscall.Flock(int(ls.lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		ls.lockFile.Close()
		ls.lockFile = nil
		if err == syscall.EWOULDBLOCK {
			return fmt.Errorf("file is locked by another process")
		}
		return err
	}

	return nil
}

// releaseLock releases the file lock
func (ls *LocalStorage[T]) releaseLock() error {
	if ls.lockFile == nil {
		return nil
	}

	err := syscall.Flock(int(ls.lockFile.Fd()), syscall.LOCK_UN)
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	err = ls.lockFile.Close()
	if err != nil {
		return fmt.Errorf("failed to close lock file: %w", err)
	}

	ls.lockFile = nil
	return nil
}

func (ls *LocalStorage[T]) getFileSizeInKB() (float64, error) {
	fileInfo, err := os.Stat(ls.file)
	if err != nil {
		return 0, fmt.Errorf("failed to get file info: %w", err)
	}

	fileSizeBytes := fileInfo.Size()

	fileSizeKB := float64(fileSizeBytes) / 1024

	// fmt.Printf("File Size in Kb %.2f", fileSizeKB)

	return fileSizeKB, nil
}
