package main

import (
	"encoding/json"
	"fmt"
	"local-key-value-DB/dbError"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type LocalStorage[T any] struct {
	filePath string
	lockFile *os.File
}

func NewLocalStorage[T any](fileName string, dir string, dataToLoad *map[string]DbData[T]) (*LocalStorage[T], error) {
	if len(strings.TrimSpace(dir)) == 0 {
		curDir, osErr := os.Getwd()
		if osErr != nil {
			return nil, osErr
		}
		dir = curDir
	}
	fileName, fileErr := ValidateAndFixJSONFilename(fileName)
	if fileErr != nil {
		return nil, fileErr
	}
	filePath := filepath.Join(dir, fileName)
	localStorage := &LocalStorage[T]{
		filePath: filePath,
	}

	fileExists, err := localStorage.fileExists(dir)
	if err != nil {
		return nil, err
	}
	if !fileExists {
		if err := localStorage.createFile(); err != nil {
			return nil, dbError.FailedToCreateFile("")
		}
	} else {
		if err := localStorage.Load(dataToLoad); err != nil {
			return nil, dbError.FailedToLoadFile("")
		}
	}
	// Acquire lock immediately when creating database
	if err := localStorage.acquireLock(); err != nil {
		return nil, dbError.FailedToAcquireLock(fmt.Sprintf("%s", err))
	}
	// or else throw error
	return localStorage, nil
}

func (ls *LocalStorage[T]) createFile() error {
	dir := filepath.Dir(ls.filePath)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return dbError.FailedToCreateDirectory(fmt.Sprintf("%s", err))
		}
	} else if err != nil {
		return dbError.FailedToCheckDir(fmt.Sprintf("%s", err))
	}

	// fmt.Println("Creating file at:", ls.filePath)
	file, err := os.Create(ls.filePath)
	if err != nil {
		return dbError.FailedToCreateFile(fmt.Sprintf("%s", err))
	}
	defer file.Close()

	// Initialize the file with an empty map
	return ls.Sync(make(map[string]DbData[T]))
}
func (ls *LocalStorage[T]) fileExists(dir string) (bool, error) {

	_, dirErr := os.Stat(dir)

	if os.IsNotExist(dirErr) {
		return false, dbError.DirectoryNotExists("")
	}

	_, err := os.Stat(ls.filePath)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, dbError.FailedToCheckFileExists(fmt.Sprintf("%s", err))
}

func (ls *LocalStorage[T]) Sync(data map[string]DbData[T]) error {
	// fmt.Printf("Sync data %+v\n ", data)
	file, err := os.Create(ls.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	return encoder.Encode(data)
}

func (ls *LocalStorage[T]) Load(dataToLoad *map[string]DbData[T]) error {
	file, err := os.Open(ls.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	return decoder.Decode(&dataToLoad)
}

func (ls *LocalStorage[T]) acquireLock() error {
	var err error
	ls.lockFile, err = os.OpenFile(ls.filePath+".lock", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	err = syscall.Flock(int(ls.lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		ls.lockFile.Close()
		ls.lockFile = nil
		if err == syscall.EWOULDBLOCK {
			return dbError.FileIsLockedByAnotherProcess("")
		}
		return err
	}

	return nil
}

func (ls *LocalStorage[T]) releaseLock() error {
	if ls.lockFile == nil {
		return nil
	}

	err := syscall.Flock(int(ls.lockFile.Fd()), syscall.LOCK_UN)
	if err != nil {
		return dbError.FailedToReleaseLock(fmt.Sprintf("%s", err))
	}

	err = ls.lockFile.Close()
	if err != nil {
		return dbError.FailedToCloseLockedFile(fmt.Sprintf("%s", err))
	}

	ls.lockFile = nil
	return nil
}

func (ls *LocalStorage[T]) getFileSizeInKB() (float64, error) {
	fileInfo, err := os.Stat(ls.filePath)
	if err != nil {
		return 0, dbError.FailedToGetFileInfo(fmt.Sprintf("%s", err))
	}

	fileSizeBytes := fileInfo.Size()

	fileSizeKB := float64(fileSizeBytes) / 1024

	// fmt.Printf("File Size in Kb %.2f", fileSizeKB)

	return fileSizeKB, nil
}
