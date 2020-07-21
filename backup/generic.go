package backup

type file_info struct {
	Size uint64       `json:"size"`
	BSize uint64      `json:"bsize"`
	Sha1  string      `json:"sha1"`
}
type table_info struct {
	Size uint64       `json:"size"`
	BSize uint64      `json:"bsize"`
	RepoSize uint64   `json:"repo_size"`
	RepoBSize uint64  `json:"repo_bsize"`
	DbDir string      `json:"db_dir"`
	TableDir string   `json:"table_dir"`
	Partitions []string `json:"partitions"`
	Dirs []string       `json:"dirs"`
	Files map[string]file_info `json:"files"`
}
type database_info_v1 struct {
	Size uint64       `json:"size"`
	BSize uint64      `json:"bsize"`
	RepoSize uint64   `json:"repo_size"`
	RepoBSize uint64  `json:"repo_bsize"`
	Tables map[string]table_info  `json:"tables"`
	MetaData map[string]file_info `json:"metadata"`
}
type database_info_v2 struct {
	Size uint64       `json:"size"`
	BSize uint64      `json:"bsize"`
	RepoSize uint64   `json:"repo_size"`
	RepoBSize uint64  `json:"repo_bsize"`
	Tables []string   `json:"tables"`
	MetaData map[string]file_info `json:"metadata"`
}
type backup_info_v1 struct {
	Size uint64       `json:"size"`
	BSize uint64      `json:"bsize"`
	RepoSize uint64   `json:"repo_size"`
	RepoBSize uint64  `json:"repo_bsize"`
	Name string       `json:"name"`
	Type string       `json:"type"`
	Version uint      `json:"version"`
	StartDate string  `json:"start_date"`
	StopDate string   `json:"stop_date"`
	Reference []string `json:"reference"`
	DBS map[string]database_info_v1 `json:"dbs"`
	BackupFilter map[string][]string `json:"filter"`
}
type backup_info_v2 struct {
	Size uint64       `json:"size"`
	BSize uint64      `json:"bsize"`
	RepoSize uint64   `json:"repo_size"`
	RepoBSize uint64  `json:"repo_bsize"`
	Name string       `json:"name"`
	Type string       `json:"type"`
	Version uint      `json:"version"`
	StartDate string  `json:"start_date"`
	StopDate string   `json:"stop_date"`
	Reference []string `json:"reference"`
	DBS map[string]database_info_v2 `json:"dbs"`
	BackupFilter map[string][]string `json:"filter"`
}

// Contains tells whether a contains x.
func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}
