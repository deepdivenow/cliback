package transport

import (
	"cliback/config"
	"errors"
	"hash"
	"io"
	"os"
)

type transportCloser struct {
	Closer []io.Closer
}

func (t *transportCloser) Close() error {
	for i := len(t.Closer) - 1; i >= 0; i-- {
		_ = t.Closer[i].Close()
	}
	return nil
}

type Stater interface {
	Stat() (os.FileInfo, error)
}
type Flusher interface {
	Flush() error
}

type transport struct {
	Size    int64
	BSize   int64
	Reader  io.Reader
	Writer  io.Writer
	Sha1Sum hash.Hash
	Closer  []io.Closer
	Stater  [2]Stater
	Flusher []Flusher
	Ready   bool
	Closed  bool
}

func (t *transport) Flush() error {
	for _, f := range t.Flusher {
		_ = f.Flush()
	}
	return nil
}

func (t *transport) Close() error {
	if t.Closed {
		return nil
	}
	for i := len(t.Closer) - 1; i >= 0; i-- {
		_ = t.Closer[i].Close()
	}
	t.Closed=true
	return nil
}

func (t *transport) Cleanup() error {
	if t.Ready {
		return nil
	}
	return t.Close()
}

func (t *transport) Copy() (int64, error) {
	num, err := io.Copy(t.Writer, t.Reader)
	if err != nil {
		return num, err
	}
	_ = t.Flush()
	s, err := t.Stater[0].Stat()
	if err == nil {
		t.Size = s.Size()
	}
	d, err := t.Stater[1].Stat()
	if err == nil {
		t.BSize = d.Size()
	}
	return num, nil
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
