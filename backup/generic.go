package backup

import (
	"cliback/config"
	"cliback/database"
	"cliback/transport"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"
)

func (lhs *counter) Add(rhs *counter) {
	lhs.Size += rhs.Size
	lhs.BSize += rhs.BSize
	lhs.RepoSize += rhs.RepoSize
	lhs.RepoBSize += rhs.RepoBSize
}

func (ti *tableInfo) Add(fi *fileInfo, fname string) {
	ti.Files[fname] = *fi
	ti.Size += fi.Size
	ti.BSize += fi.BSize
	if len(fi.Reference) < 1 {
		ti.RepoSize += fi.Size
		ti.RepoBSize += fi.BSize
	} else {
		if !Contains(ti.Reference, fi.Reference) {
			ti.Reference = append(ti.Reference, fi.Reference)
		}
	}
	if len(fi.Storage) > 0 {
		if !(Contains(ti.Storages, fi.Storage)) {
			ti.Storages = append(ti.Storages, fi.Storage)
		}
	}
}

func (ti *tableInfo) AddJob(j *transport.CliFile) {
	storage := j.Storage
	if storage == "default" {
		storage = ""
	}
	ti.Files[j.Name] = fileInfo{
		Size:      j.Size,
		BSize:     j.BSize,
		Sha1:      j.Sha1,
		Reference: j.Reference,
		Storage:   storage,
	}
	ti.Size += j.Size
	ti.BSize += j.BSize
	if len(j.Reference) > 1 {
		if !Contains(ti.Reference, j.Reference) {
			ti.Reference = append(ti.Reference, j.Reference)
		}
	} else {
		ti.RepoSize += j.Size
		ti.RepoBSize += j.BSize
	}
}

func (di *databaseInfo) Add(ti *tableInfo) {
	di.counter.Add(&ti.counter)
	for _, r := range ti.Reference {
		if !Contains(di.Reference, r) {
			di.Reference = append(di.Reference, r)
		}
	}
}

func (bi *backupInfo) Add(di *databaseInfo) {
	bi.counter.Add(&di.counter)
	for _, r := range di.Reference {
		if !Contains(bi.Reference, r) {
			bi.Reference = append(bi.Reference, r)
		}
	}
}

type counter struct {
	Size      int64 `json:"size"`
	BSize     int64 `json:"bsize"`
	RepoSize  int64 `json:"repo_size"`
	RepoBSize int64 `json:"repo_bsize"`
}

type fileInfo struct {
	Size      int64  `json:"size"`
	BSize     int64  `json:"bsize"`
	Sha1      string `json:"sha1"`
	Reference string `json:"reference,omitempty"`
	Storage   string `json:"storage,omitempty"`
}
type tableInfo struct {
	counter
	DbDir        string              `json:"db_dir"`
	TableDir     string              `json:"table_dir"`
	BackupStatus string              `json:"backup_status"`
	Partitions   []string            `json:"partitions"`
	Dirs         []string            `json:"dirs"`
	Files        map[string]fileInfo `json:"files"`
	MetaData     fileInfo            `json:"metadata"` // Will be Used in v2
	Reference    []string            `json:"reference,omitempty"`
	Storages     []string            `json:"storages,omitempty"`
}
type databaseInfo struct {
	counter
	Tables    map[string]tableInfo `json:"tables"`
	MetaData  map[string]fileInfo  `json:"metadata"`
	Reference []string             `json:"reference,omitempty"`
}
type backupInfo struct {
	counter
	Name         string                  `json:"name"`
	Type         string                  `json:"type"`
	Version      uint                    `json:"version"`
	StartDate    string                  `json:"start_date"`
	StopDate     string                  `json:"stop_date"`
	Reference    []string                `json:"reference,omitempty"`
	DBS          map[string]databaseInfo `json:"dbs"`
	BackupFilter map[string][]string     `json:"filter"`
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

// Position say substring position
func Position(a []string, x string) (int, error) {
	for i, n := range a {
		if x == n {
			return i, nil
		}
	}
	return 0, errors.New("Substring not found")
}

// GetFormatedTime return current time in formated style
func GetFormatedTime() string {
	t := time.Now()
	formatted := fmt.Sprintf("%04d%02d%02d_%02d%02d%02d",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second())
	return formatted
}

func GenerateBackupName() string {
	c := config.New()
	return GetFormatedTime() + strings.ToUpper(c.TaskArgs.BackupType[:1])
}

func GetDirs(p string) ([]string, error) {
	var result []string
	files, err := ioutil.ReadDir(p)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.IsDir() {
			result = append(result, f.Name())
		}
	}
	return result, nil
}

func GetDirsInShadow(tInfo database.TableInfo) []string {
	var result []string
	c := config.New()
	for storage := range c.ClickhouseStorage {
		res, err := GetDirs(path.Join(c.GetShadow(storage), tInfo.GetShortPath()))
		if err != nil {
			continue
		}
		result = append(result, res...)
	}
	return result
}

func CheckStorage() error {
	c := config.New()
	if c.ClickhouseStorage != nil {
		return nil
	}
	ch := database.New()
	chStore, err := ch.GetDisks()
	if err != nil {
		return err
	}
	c.ClickhouseStorage = chStore
	return nil
}

func SplitShadow(p string) ([]string, error) {
	dirs := strings.Split(p, "/")
	pos := -1
	var err error
	for _, spliter := range []string{"data", "store"} {
		pos, err = Position(dirs, spliter)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	resultShadow := strings.Join(dirs[0:pos], "/")
	resultPath := strings.Join(dirs[pos:pos+3], "/")
	resultFile := strings.Join(dirs[pos+3:], "/")
	return []string{resultShadow, resultPath, resultFile}, nil
}

func RemoveShadowDirs() {
	c := config.New()
	for storage := range c.ClickhouseStorage {
		shDir := c.GetShadow(storage)
		st, err := os.Stat(shDir)
		if err != nil {
			continue
		}
		if st.IsDir() {
			os.RemoveAll(shDir)
		}
	}
}
