package backup

import (
	"cliback/config"
	"cliback/transport"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"
)



func (lhs *counter) Add (rhs *counter){
	lhs.Size+=rhs.Size
	lhs.BSize+=rhs.BSize
	lhs.RepoSize+=rhs.RepoSize
	lhs.RepoBSize+=rhs.RepoBSize
}

func (ti *table_info) Add (fi *file_info,fname string){
	ti.Files[fname]=*fi
	ti.Size+=fi.Size
	ti.BSize+=fi.BSize
	if len(fi.Reference) < 1{
		ti.RepoSize+=fi.Size
		ti.RepoBSize+=fi.BSize
	} else {
		if !Contains(ti.Reference, fi.Reference) {
			ti.Reference = append(ti.Reference, fi.Reference)
		}
	}
}

func (ti *table_info) AddJob (j *transport.CliFile){
	ti.Files[j.Name]=file_info{
		Size:  j.Size,
		BSize: j.BSize,
		Sha1:  j.Sha1,
		Reference: j.Reference,
	}
	ti.Size+=j.Size
	ti.BSize+=j.BSize
	if len(j.Reference) > 1{
		if !Contains(ti.Reference, j.Reference) {
			ti.Reference = append(ti.Reference, j.Reference)
		}
	} else {
		ti.RepoSize+=j.Size
		ti.RepoBSize+=j.BSize
	}
}

func (di *database_info) Add (ti *table_info){
	di.counter.Add(&ti.counter)
	for _,r := range(ti.Reference){
		if !Contains(di.Reference,r){
			di.Reference=append(di.Reference, r)
		}
	}
}

func (bi *backup_info) Add (di *database_info){
	bi.counter.Add(&di.counter)
	for _,r := range(di.Reference){
		if !Contains(bi.Reference,r){
			bi.Reference=append(bi.Reference, r)
		}
	}
}

type counter struct {
	Size int64       `json:"size"`
	BSize int64      `json:"bsize"`
	RepoSize int64   `json:"repo_size"`
	RepoBSize int64  `json:"repo_bsize"`
}

type file_info struct {
	Size int64       `json:"size"`
	BSize int64      `json:"bsize"`
	Sha1  string      `json:"sha1"`
	Reference string  `json:"reference,omitempty"`
}
type table_info struct {
	counter
	DbDir string      `json:"db_dir"`
	TableDir string   `json:"table_dir"`
	BackupStatus string `json:"backup_status"`
	Partitions []string `json:"partitions"`
	Dirs []string       `json:"dirs"`
	Files map[string]file_info `json:"files"`
	MetaData file_info `json:"metadata"`
	Reference []string `json:"reference,omitempty"`
}
type database_info struct {
	counter
	Tables map[string]table_info  `json:"tables"`
	MetaData map[string]file_info `json:"metadata"`
	Reference []string `json:"reference,omitempty"`
}
type backup_info struct {
	counter
	Name string       `json:"name"`
	Type string       `json:"type"`
	Version uint      `json:"version"`
	StartDate string  `json:"start_date"`
	StopDate string   `json:"stop_date"`
	Reference []string `json:"reference,omitempty"`
	DBS map[string]database_info `json:"dbs"`
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
func Position(a []string, x string) (int,error) {
	for i, n := range a {
		if x == n {
			return i,nil
		}
	}
	return 0,errors.New("Substring not found")
}

func GetFormatedTime() string{
	t := time.Now()
	formatted := fmt.Sprintf("%04d%02d%02d_%02d%02d%02d",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second())
	return formatted
}

func GenerateBackupName() string{
	c:=config.New()
	return GetFormatedTime()+strings.ToUpper(c.TaskArgs.BackupType[:1])
}

func GetDirs(p string) ([]string, error){
	var result []string
	files, err := ioutil.ReadDir(p)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.IsDir(){
			result=append(result,f.Name())
		}
	}
	return result,nil
}

func SplitShadow(p string)([]string, error){
	dirs:=strings.Split(p,"/")
	pos,err:=Position(dirs,"data")
	if err != nil {
		return nil, err
	}
	result_shadow:=strings.Join(dirs[0:pos+1],"/")
	result_path:=strings.Join(dirs[pos+1:pos+3],"/")
	result_file:=strings.Join(dirs[pos+3:],"/")
	return []string{result_shadow,result_path,result_file},nil
}