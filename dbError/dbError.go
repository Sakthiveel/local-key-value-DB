package dbError

import "fmt"

// Custom error type for DB errors
type DBError struct {
	Message        string
	AdditionalInfo string
}

func (e *DBError) Error() string {
	return fmt.Sprintf("%s , %s", e.Message, e.AdditionalInfo)
}

// Factory functions for common errors
func NewDBError(msg string, additionalInfo string) error {
	return &DBError{
		Message:        msg,
		AdditionalInfo: additionalInfo,
	}
}

func ErrDBConnectionFailed(info string) error {
	return NewDBError("Failed to connect to the database", info)
}

func ErrDataNotFound(info string) error {
	return NewDBError("Data not found", info)
}

func ErrDBTimeout(info string) error {
	return NewDBError("Database operation timed out", info)
}

func ReadOperationFailed(info string) error {
	return NewDBError("Read operation failed", info)
}

func WriteOperationFailed(info string) error {
	return NewDBError("Write operation failed", info)
}

func DeleteOperationFailed(info string) error {
	return NewDBError("Delete operation failed", info)
}

func EntryExpired(info string) error {
	return NewDBError("Entry Expired", info)
}

func KeyExpired(info string) error {
	return NewDBError("Key Expired", info)
}

func InvalidFileName(info string) error {
	return NewDBError("Invalid File Name", info)
}

func DatabaseAlreadyClose(info string) error {
	return NewDBError("Database alread closed", info)
}

func UnkownOperation(info string) error {
	return NewDBError("Unkown operation", info)
}

func EntryAlreadyExists(info string) error {
	return NewDBError("Entry already exists", info)
}

func FailedToAcquireLock(info string) error {
	return NewDBError("Failed to acquire lock", info)
}
func BatchLimitCountExceeds(info string) error {
	return NewDBError("Batch operation exceeds allowed maxium allowed entries", info)
}
func BatchSizeLimitCrossed(info string) error {
	return NewDBError("Batch size exceeds storage limit", info)
}

func DirectoryNotExists(info string) error {
	return NewDBError("Direcotyr not exists", info)
}

func FailedToReleaseLock(info string) error {
	return NewDBError("Failed to release the file lock", info)
}

func FailedToCheckFileExists(info string) error {
	return NewDBError("Failed to check if file exists", info)
}

func FailedToCreateDirectory(info string) error {
	return NewDBError("Failed to create directory", info)
}

func FailedToConvertMapToJson(info string) error {
	return NewDBError("Faield to convert map to json", info)
}

func JsonSizeExceedsLimit(info string) error {
	return NewDBError("Json Size exceed Limit", info)
}

func EntrySizeExceedsLimit(info string) error {
	return NewDBError("Entry size exceeds limit ", info)
}

func FailedToCreateFile(info string) error {
	return NewDBError("Failed to create file", info)
}

func NotAvailabeSpace(info string) error {
	return NewDBError("Not available space to complete the  operation", info)
}

func KeyNotFound(info string) error {
	return NewDBError("Key not found", info)
}

func DBAlreadyClosed(info string) error {
	return NewDBError("DB already closed", info)
}

func KeySizeExceedsLimit(limit int, info string) error {
	return NewDBError(fmt.Sprintf("Key size exceeds maximum length of %v characters", limit), info)
}

func FailedToGetFileSize(info string) error {
	return NewDBError("Failed to get file size", info)
}
func FailedToCheckDir(info string) error {
	return NewDBError("Failed to check directory", info)
}

func FileIsLockedByAnotherProcess(info string) error {
	return NewDBError("File is locked up another process", info)
}
func FailedToCloseLockedFile(info string) error {
	return NewDBError("Failed to close locked file", info)
}

func FailedToGetFileInfo(info string) error {
	return NewDBError("Failed to get file info", info)
}

func FailedToLoadFile(info string) error {
	return NewDBError("Faield to load file", info)
}
