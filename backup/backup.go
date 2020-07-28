package backup

import (
	"cliback/config"
	"cliback/database"
	"cliback/transport"
	"cliback/workerpool"
	"encoding/hex"
	"errors"
	"log"
	"os"
	"path"
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

func BackupRun(cf transport.CliFile) (transport.CliFile, error) {
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
	c:=config.New()
	ch:=database.New()
	ch.SetDSN(c.ClickhouseBackupConn)
	backup_objects,err:=get_backup_objects()
	if err != nil{
		return err
	}
	if len(c.TaskArgs.JobName) < 1 {
		c.TaskArgs.JobName=GenerateBackupName()
	}
	log.Printf("Backup Job Name: %s", c.TaskArgs.JobName)

	bi:=backup_info{
		Name:         c.TaskArgs.JobName,
		Type:         c.TaskArgs.BackupType,
		Version:      1,
		BackupFilter: backup_objects,
		StartDate: GetFormatedTime(),
		DBS: make(map[string]database_info),
	}
	for db,tables := range(backup_objects){
		di:=database_info{
			Tables:    make(map[string]table_info),
			MetaData:  nil,
		}
		for _,table := range(tables){
			log.Printf("%s/%s",db,table)
			ti,_:=backupTable(db,table,"")
			di.Tables[table]=ti
		}
		bi.DBS[db]=di
	}
	return nil
}

func backupTable(db,table,part string)(table_info,error)  {
	c:=config.New()
	ch:=database.New()
	parts,err:=ch.GetPartitions(db,table,part)
	if err != nil{
		return table_info{BackupStatus: "bad"}, err
	}
	r,err:=ch.GetFNames(db,table,part)
	if err != nil{
		return table_info{BackupStatus: "bad"}, err
	}
	ti:=table_info{
		Size:       0,
		BSize:      0,
		RepoSize:   0,
		RepoBSize:  0,
		DbDir:      r[0],
		TableDir:   r[1],
		Partitions: parts,
		//Dirs:       nil,
		Files:      map[string]file_info{},
		BackupStatus: "bad",
	}
	err=ch.FreezeTable(db,table,part)
	if err != nil{
		return table_info{BackupStatus: "bad"}, err
	}
	shDir,err:=ch.GetIncrement()
	if err != nil{
		return table_info{BackupStatus: "bad"}, err
	}
	c.ShadowDir=path.Join(c.ClickhouseDir,"shadow",strconv.Itoa(shDir))
	var wp_task workerpool.TaskFunc = func(i interface{}) (interface{}, error) {
		field, _ := i.(transport.CliFile)
		return BackupRun(field)
	}

	wp := workerpool.MakeWorkerPool(wp_task, 4, 3, 10)
	wp.Start()
	go FindFiles(c.ShadowDir, wp.Get_Jobs_Chan())

	for job := range wp.Get_Results_Chan() {
		j, _ := job.(transport.CliFile)
		ti.Files[j.Name]=file_info{
			Size:  j.Size,
			BSize: j.BSize,
			Sha1:  j.Sha1,
		}
		ti.Size+=j.Size
		ti.BSize+=j.BSize
	}
	return ti,nil
}

func get_backup_objects() (map[string][]string,error) {
	backup_objects:=map[string][]string{}
	c:=config.New()
	backup_filter:=c.BackupFilter
	//var backup_filter map[string][]string
	//if c.BackupFilter == nil{
	//	backup_filter=make(map[string][]string)
	//} else {
	//	backup_filter=c.BackupFilter
	//}
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
	//sp:=sftp_pool.New()
	//Move to backup/restore function
	//sp.SetSSHConfig(map[string]string{
	//	"user": c.BackupStorage.BackupConn.UserName,
	//	"pass": c.BackupStorage.BackupConn.Password,
	//	"remote": c.BackupStorage.BackupConn.HostName,
	//	"port": ":"+strconv.Itoa(int(c.BackupStorage.BackupConn.Port)),
	//	"public_key": c.BackupStorage.BackupConn.KeyFilename,
	//})
	///

	var wp_task workerpool.TaskFunc = func(i interface{}) (interface{}, error) {
		field, _ := i.(transport.CliFile)
		return BackupRun(field)
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
