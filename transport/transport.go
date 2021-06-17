package transport

import (
	"cliback/config"
	"errors"
	"regexp"
)

var (
	errTransCreate  = errors.New("Transport not created")
	errBadTransType = errors.New("Meta Read bad transport type")
)

type Transport interface {
	Do(file CliFile) (*TransportStat, error)
	ReadMeta(mf *MetaFile) error
	WriteMeta(mf *MetaFile) error
	SearchMeta() ([]string, error)
	DeleteBackup(backupName string) error
}

// Transport for backup/restore files
type TransportStat struct {
	Size    int64
	BSize   int64
	Sha1Sum string
}

// MakeTransport archive file and returns meta info
func MakeTransport() (Transport, error) {
	c := config.New()
	var t Transport
	switch c.BackupStorage.Type {
	case "local":
		t = new(TransportLocal)
	case "sftp":
		t = new(TransportSFTP)
	case "command":
		t = new(TransportCommand)
	case "webdav":
		t = new(TransportWebDav)
	default:
		return nil, errTransCreate
	}
	return t, nil
}

func metaDirNameMatched(metaDirName string) bool {
	if reMatch, _ := regexp.MatchString("^(\\d{8}_\\d{6}[FDIP]{1})$", metaDirName); reMatch {
		return true
	}
	return false
}
