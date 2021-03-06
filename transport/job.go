package transport

import (
	"bytes"
	"cliback/config"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"log"
	"os"
	"path"
)

// RunJobType backup or restore
type RunJobType int

// RunJobType backup or restore
const (
	Backup RunJobType = iota + 1
	Restore
)

// CliFile struct for descript each Clickhouse Table file
type CliFile struct {
	Size       int64
	BSize      int64
	Name       string
	Path       string
	DBName     string
	TableName  string
	Reference  string
	Shadow     string
	Storage    string
	RunJobType RunJobType
	TryRetry   bool
	Sha1       string
}

// Archive returns archive file name
func (cf *CliFile) Archive() string {
	c := config.New()
	if len(cf.Reference) > 0 {
		return path.Join(cf.Reference, cf.DBName, cf.TableName, cf.Name+".gz")
	}
	return path.Join(c.TaskArgs.JobName, cf.DBName, cf.TableName, cf.Name+".gz")
}

///need refactor
// RestoreDest returns restore path for table file
func (cf *CliFile) RestoreDest() string {
	c := config.New()
	store := cf.Storage
	if len(cf.Storage) < 1 {
		store = "default"
	}
	if storagePath, ok := c.ClickhouseStorage[store]; ok {
		return path.Join(storagePath, cf.Path, "detached", cf.Name)
	}
	if c.ClickhouseRestoreOpts.BadStorageToDefault {
		store = "default"
		if storagePath, ok := c.ClickhouseStorage[store]; ok {
			return path.Join(storagePath, cf.Path, "detached", cf.Name)
		}
	}
	if c.ClickhouseRestoreOpts.FailIfStorageNotExists {
		log.Fatal("ERR: Bad storage: ", store)
	}
	return ""
}

// BackupSrc returns full file path for backup
func (cf *CliFile) BackupSrc() string {
	return path.Join(cf.Shadow, cf.Path, cf.Name)
}

// BackupSrcShort returns short file path for backup
func (cf *CliFile) BackupSrcShort() string {
	return path.Join(cf.Path, cf.Name)
}

// Sha1Compute compute sha1 for file
func (cf *CliFile) Sha1Compute() error {
	source, err := os.Open(cf.BackupSrc())
	if err != nil {
		return err
	}
	defer source.Close()
	Sha1Sum := sha1.New()
	_, err = io.Copy(Sha1Sum, source)
	if err != nil {
		return err
	}
	cf.Sha1 = hex.EncodeToString(Sha1Sum.Sum(nil))
	return nil
}

// MetaFile struct for save/load backup meta info
type MetaFile struct {
	Size     int64
	BSize    int64
	Name     string
	Path     string
	JobName  string
	TryRetry bool
	Sha1     string
	Content  bytes.Buffer
}

// Archive returns archive file path for metafile
func (mf *MetaFile) Archive() string {
	return path.Join(mf.JobName, mf.Path, mf.Name+".gz")
}

// SPath returns file path for old type metafile
func (mf *MetaFile) SPath() string {
	return path.Join(mf.JobName, mf.Path, mf.Name)
}
