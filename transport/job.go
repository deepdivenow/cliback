package transport

import (
	"bytes"
	"cliback/config"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path"
)

type RunJobType int

const (
	Backup RunJobType = iota + 1
	Restore
)

type CliFile struct {
	Size       int64
	BSize      int64
	Name       string
	Path       string
	Reference  string
	Shadow     string
	Storage    string
	RunJobType RunJobType
	TryRetry   bool
	Sha1       string
}

func (cf *CliFile) Archive() string {
	c := config.New()
	if len(cf.Reference) > 0 {
		return path.Join(cf.Reference, cf.Path, cf.Name+".gz")
	}
	return path.Join(c.TaskArgs.JobName, cf.Path, cf.Name+".gz")
}
func (cf *CliFile) RestoreDest() string {
	c := config.New()
	store := cf.Storage
	if len(cf.Storage) < 1 {
		store = "default"
	}
	if storagePath,ok := c.ClickhouseStorage[store]; ok{
		return path.Join(storagePath, "data", cf.Path, "detached", cf.Name)
	}
	if c.ClickhouseRestoreOpts.BadStorageToDefault {
		store = "default"
		if storagePath, ok := c.ClickhouseStorage[store]; ok {
			return path.Join(storagePath, "data", cf.Path, "detached", cf.Name)
		}
	}
	if c.ClickhouseRestoreOpts.FailIfStorageNotExists{
		log.Fatal(fmt.Sprint("ERR: Bad storage: %s", store))
	}
	return ""
}
func (cf *CliFile) BackupSrc() string {
	return path.Join(cf.Shadow, cf.Path, cf.Name)
}
func (cf *CliFile) BackupSrcShort() string {
	return path.Join(cf.Path, cf.Name)
}
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

func (mf *MetaFile) Archive() string {
	return path.Join(mf.JobName, mf.Path, mf.Name+".gz")
}

func (mf *MetaFile) SPath() string {
	return path.Join(mf.JobName, mf.Path, mf.Name)
}
