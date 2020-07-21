package backup

import (
	"cliback/config"
	"cliback/database"
	"cliback/sftp_pool"
	"cliback/transport"
	"cliback/workerpool"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

func FindFiles(dir_for_backup string, jobs_chan chan<- workerpool.TaskElem) {
	err := filepath.Walk(dir_for_backup,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			cPath := path[len(dir_for_backup):]
			cliF := transport.CliFile{
				Name:       cPath,
				Path:       cPath,
				RunJobType: transport.Backup,
				TryRetry:   false,
			}
			jobs_chan <- cliF
			return nil
		})
	if err != nil {
		log.Println(err)
	}
	close(jobs_chan)
}

func Run(cf transport.CliFile) (transport.CliFile, error) {
	tr, err := transport.MakeTransport(cf)
	if err != nil {
		return transport.CliFile{}, err
	}
	defer tr.Close()
	_, err = tr.Copy()
	// Add copied check
	if err != nil {
		return transport.CliFile{}, err
	}
	cf.Sha1 = hex.EncodeToString(tr.Sha1Sum.Sum(nil))
	return cf, nil
}

func Backup() error{
	// Main backup loop
	//var backup_objects map[string][]string
	backup_objects,err:=get_backup_objects()
	if err != nil{
		return err
	}
	for db,tables := range(backup_objects){
		for _,table := range(tables){
			fmt.Printf("%s/%s",db,table)
			backup_table(db,table,"")
		}
	}
	return nil
}

func backup_table(db,table,part string)(error)  {
	return nil
}

func get_backup_objects() (map[string][]string,error) {
	var backup_objects map[string][]string
	c:=config.New()
	backup_filter:=c.BackupFilter
	ch:=database.New()
	C_DBS,err := ch.GetDBS()
	if err != nil {
		return nil, err
	}
	for _,db := range C_DBS{
		C_Tables,err:=ch.GetTables(db)
		if err != nil {
			return nil, err
		}
		//clone slice
		backup_objects[db]=append(C_Tables[:0:0], C_Tables...)
	}
	if backup_filter == nil {
		return backup_objects, nil
	}
	for db,tables := range backup_filter{
		for _,table := range tables {
			if !Contains(backup_objects[db],table){
				return nil, errors.New("Bad filter, not contains in database")
			}
		}
	}
	return backup_filter, nil
}

func Backup_OLD() {
	c := config.New()
	c.Read("/home/dro/go-1.13/src/cliback/clickhouse_backup.yaml")
	c.SetShadow("/home/dro/.thunderbird")
	sp:=sftp_pool.New()
	//Move to backup/restore function
	sp.SetSSHConfig(map[string]string{
		"user": c.BackupStorage.BackupConn.UserName,
		"pass": c.BackupStorage.BackupConn.Password,
		"remote": c.BackupStorage.BackupConn.HostName,
		"port": ":"+strconv.Itoa(int(c.BackupStorage.BackupConn.Port)),
		"public_key": c.BackupStorage.BackupConn.KeyFilename,
	})
	///

	var wp_task workerpool.TaskFunc = func(i interface{}) (interface{}, error) {
		field, _ := i.(transport.CliFile)
		return Run(field)
	}

	wp := workerpool.MakeWorkerPool(wp_task, 4, 3, 10)
	wp.Start()
	go FindFiles(c.ShadowDir, wp.Get_Jobs_Chan())
	for job := range wp.Get_Results_Chan() {
		j, _ := job.(transport.CliFile)
		println(j.Name, j.Sha1)
	}
}

//config_map  := map[string]string{
//"user": "dro",
//"pass": "dctulfq1",
//"remote": "stich",
//"port": ":22",
//}
