package transport

import (
	"cliback/config"
	"errors"
	"regexp"
)

var (
	errTransCreate = errors.New("Transport not created")
	errBadTransType = errors.New("Meta Read bad transport type")
)
// Transport for backup/restore files
type Transport struct {
	Size    int64
	BSize   int64
	Sha1Sum string
}
// MakeTransport archive file and returns meta info
func MakeTransport(file CliFile) (*Transport, error) {
	c := config.New()
	if file.RunJobType == Backup {
		switch c.BackupStorage.Type {
		case "local":
			return MakeBackupTransportLocal(file)
		case "sftp":
			return MakeBackupTransportSFTP(file)
		default:
			return nil, errTransCreate
		}
	}
	if file.RunJobType == Restore {
		switch c.BackupStorage.Type {
		case "local":
			return MakeRestoreTransportLocal(file)
		case "sftp":
			return MakeRestoreTransportSFTP(file)
		default:
			return nil, errTransCreate
		}
	}
	return nil, errTransCreate
}
// ReadMeta restore backup metafile and returns meta info
func ReadMeta(mf *MetaFile) error {
	c := config.New()
	switch c.BackupStorage.Type {
	case "local":
		return ReadMetaLocal(mf)
	case "sftp":
		return ReadMetaSFTP(mf)
	default:
		return errBadTransType
	}
}
// WriteMeta archive backup metafile and returns meta info
func WriteMeta(mf *MetaFile) error {
	c := config.New()
	switch c.BackupStorage.Type {
	case "local":
		return WriteMetaLocal(mf)
	case "sftp":
		return WriteMetaSFTP(mf)
	default:
		return errBadTransType
	}
}
// SearchMeta search & returns backup names in archive
func SearchMeta() ([]string, error) {
	c := config.New()
	switch c.BackupStorage.Type {
	case "local":
		return SearchMetaLocal()
	case "sftp":
		return SearchMetaSFTP()
	default:
		return nil, errBadTransType
	}
}
// DeleteBackup delete backup from archive
func DeleteBackup(backupName string) (error) {
	c := config.New()
	switch c.BackupStorage.Type {
	case "local":
		return DeleteBackupLocal(backupName)
	case "sftp":
		return DeleteBackupSFTP(backupName)
	default:
		return errBadTransType
	}
}

func metaDirNameMatched (metaDirName string) bool {
	if reMatch, _ := regexp.MatchString("^(\\d{8}_\\d{6}[FDIP]{1})$", metaDirName); reMatch {
		return true
	}
	return false
}