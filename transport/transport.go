package transport

import (
	"cliback/config"
	"errors"
	"regexp"
)

type transport struct {
	Size    int64
	BSize   int64
	Sha1Sum string
}

func MakeTransport(file CliFile) (*transport, error) {
	c := config.New()
	if file.RunJobType == Backup {
		switch c.BackupStorage.Type {
		case "local":
			return MakeBackupTransportLocal(file)
		case "sftp":
			return MakeBackupTransportSFTP(file)
		default:
			return nil, errors.New("Transport not created")
		}
	}
	if file.RunJobType == Restore {
		switch c.BackupStorage.Type {
		case "local":
			return MakeRestoreTransportLocal(file)
		case "sftp":
			return MakeRestoreTransportSFTP(file)
		default:
			return nil, errors.New("Transport not created")
		}
	}
	return nil, errors.New("Transport not created")
}

func ReadMeta(mf *MetaFile) error {
	c := config.New()
	switch c.BackupStorage.Type {
	case "local":
		return ReadMetaLocal(mf)
	case "sftp":
		return ReadMetaSFTP(mf)
	default:
		return errors.New("Meta Read bad transport type")
	}
}

func WriteMeta(mf *MetaFile) error {
	c := config.New()
	switch c.BackupStorage.Type {
	case "local":
		return WriteMetaLocal(mf)
	case "sftp":
		return WriteMetaSFTP(mf)
	default:
		return errors.New("Meta Read bad transport type")
	}
}

func SearchMeta() ([]string, error) {
	c := config.New()
	switch c.BackupStorage.Type {
	case "local":
		return SearchMetaLocal()
	case "sftp":
		return SearchMetaSFTP()
	default:
		return nil, errors.New("Meta Read bad transport type")
	}
}

func DeleteBackup(backupName string) (error) {
	c := config.New()
	switch c.BackupStorage.Type {
	case "local":
		return DeleteBackupLocal(backupName)
	case "sftp":
		return DeleteBackupSFTP(backupName)
	default:
		return errors.New("Meta Read bad transport type")
	}
}

func metaDirNameMatched (metaDirName string) bool {
	if reMatch, _ := regexp.MatchString("^(\\d{8}_\\d{6}[FDIP]{1})$", metaDirName); reMatch {
		return true
	}
	return false
}