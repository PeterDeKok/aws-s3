package s3

import (
	"fmt"
	"time"
)

type FileInfo struct {
	Exists       bool
	Key          string
	Size         int64
	LastModified time.Time
	Base         string
}

type SyncFileInfo struct {
	Key string

	ForceUpload bool
	Verbose     bool

	Source      FileInfo
	Destination FileInfo
}

type Operation string

const (
	OperationUpload Operation = "upload"
	OperationDelete Operation = "delete"
	OperationIgnore Operation = "ignore"
)

var OperationColored = map[Operation]string{
	OperationUpload: fmt.Sprintf("\x1b[34m%s\x1b[0m", string(OperationUpload)),
	OperationDelete: fmt.Sprintf("\x1b[31m%s\x1b[0m", string(OperationDelete)),
	OperationIgnore: fmt.Sprintf("\x1b[33m%s\x1b[0m", string(OperationIgnore)),
}

func (sfi *SyncFileInfo) Op() (Operation, string) {
	if !sfi.Source.Exists && !sfi.Destination.Exists {
		return OperationIgnore, "Ignore: source and destination do not exist"
	}

	if !sfi.Destination.Exists {
		return OperationUpload, "Upload: destination does not exist"
	}

	if !sfi.Source.Exists {
		return OperationDelete, "Delete: source does not exist"
	}

	// Both exist, figure out if the destination version should be updated

	if sfi.ForceUpload {
		return OperationUpload, "Upload: forced"
	}

	if sfi.Destination.Size != sfi.Source.Size {
		// If sizes differ, they can not be the same, so should be updated
		return OperationUpload, "Upload: size is different"
	}

	if sfi.Source.LastModified.After(sfi.Destination.LastModified) {
		// If the local instance was modified last, it should be updated
		return OperationUpload, fmt.Sprintf("Upload: source [%s] is edited after destination [%s]", sfi.Source.LastModified.Format("2006-01-02 15:04:05"), sfi.Destination.LastModified.Format("2006-01-02 15:04:05"))
	}

	return OperationIgnore, "Ignore: source and destination are (most likely) the same"
}

func (sfi *SyncFileInfo) OpColored() (string, string) {
	op, reason := sfi.Op()

	return opToOpColored(op), reason
}

func (sfi *SyncFileInfo) String() string {
	opColored, reason := sfi.OpColored()

	str := fmt.Sprintf("[%s] %s", opColored, sfi.Key)
	verbose := ""

	if sfi.Verbose {
		verbose = fmt.Sprintf("\r\n\t%s\r\n\t%s/%s\r\n\t### %s", sfi.Source.Key, sfi.Destination.Base, sfi.Destination.Key, reason)
	}

	return fmt.Sprintf("%s%s", str, verbose)
}

func opToOpColored(op Operation) string {
	if op, ok := OperationColored[op]; ok {
		return op
	}

	return string(OperationIgnore)
}
