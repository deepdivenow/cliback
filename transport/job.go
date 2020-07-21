package transport

import "bytes"

type RunJobType int

const (
	Backup RunJobType = iota + 1
	Restore
)

type CliFile struct {
	Name string
	Path string
	RunJobType RunJobType
	TryRetry bool
	Sha1 string
}

type MetaFile struct {
	Name string
	Path string
	TryRetry bool
	Sha1 string
	Content bytes.Buffer
}

func (cf *CliFile) Archive() (string)  {
	return cf.Path+".gz"
}
func (mf *MetaFile) Archive() (string)  {
	return mf.Path+".gz"
}