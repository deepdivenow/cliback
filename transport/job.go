package transport

import (
	"bytes"
	"path"
)

type RunJobType int

const (
	Backup RunJobType = iota + 1
	Restore
)

type CliFile struct {
	Size uint64
	BSize uint64
	Name string
	Path string
	RunJobType RunJobType
	TryRetry bool
	Sha1 string
}
func (cf *CliFile) Archive() (string)  {
	return path.Join(cf.Path,cf.Name+".gz")
}
func (cf *CliFile) RestoreDest() (string)  {
	return path.Join(cf.Path,"detached",cf.Name)
}

type MetaFile struct {
	Name string
	Path string
	TryRetry bool
	Sha1 string
	Content bytes.Buffer
}

func (mf *MetaFile) Archive() (string)  {
	return path.Join(mf.Path,mf.Name+".gz")
}

func (mf *MetaFile) FPath() (string)  {
	return path.Join(mf.Path,mf.Name)
}