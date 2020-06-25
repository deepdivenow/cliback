package transport

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

func (cf *CliFile) Archive() (string)  {
	return cf.Path+".gz"
}