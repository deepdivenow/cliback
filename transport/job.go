package transport

import (
	"bytes"
	"cliback/config"
	"path"
)

type RunJobType int

const (
	Backup RunJobType = iota + 1
	Restore
)

type CliFile struct {
	Size  int64
	BSize int64
	Name  string
	Path  string
	Reference string
	Shadow string
	RunJobType RunJobType
	TryRetry bool
	Sha1 string
}

func (cf *CliFile) Archive() (string)  {
	c:=config.New()
	if len (cf.Reference) > 0 {
		return path.Join(cf.Reference,cf.Path,cf.Name+".gz")
	}
	return path.Join(c.TaskArgs.JobName,cf.Path,cf.Name+".gz")
}
func (cf *CliFile) RestoreDest() (string)  {
	return path.Join(cf.Path,"detached",cf.Name)
}
func (cf *CliFile) BackupSrc() (string)  {
	return path.Join(cf.Shadow,cf.Path,cf.Name)
}
func (cf *CliFile) BackupSrcShort() (string)  {
	return path.Join(cf.Path,cf.Name)
}

type MetaFile struct {
	Size  int64
	BSize int64
	Name string
	Path string
	TryRetry bool
	Sha1 string
	Content bytes.Buffer
}

func (mf *MetaFile) Archive() (string)  {
	c:=config.New()
	return path.Join(c.TaskArgs.JobName,mf.Path,mf.Name+".gz")
}

func (mf *MetaFile) SPath() (string)  {
	c:=config.New()
	return path.Join(c.TaskArgs.JobName,mf.Path,mf.Name)
}