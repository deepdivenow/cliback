package backup

import (
	"cliback/config"
	"cliback/database"
	"cliback/transport"
	"cliback/workerpool"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"time"
)

func FindFiles(jobs_chan chan<- workerpool.TaskElem) {
	c:=config.New()
	for storage,_ := range(c.ClickhouseStorage) {
		dir_for_backup:=c.GetShadow(storage)
		err := filepath.Walk(dir_for_backup,
			func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() {
					return nil
				}
				cPath, err := SplitShadow(path)
				if err != nil {
					return nil
				}
				cliF := transport.CliFile{
					Name:       cPath[2],
					Path:       cPath[1],
					Shadow:     cPath[0],
					RunJobType: transport.Backup,
					TryRetry:   false,
					Storage:	storage,
				}
				log.Printf("Backup  From %s Archive: %s", cliF.BackupSrcShort(), cliF.Archive())
				jobs_chan <- cliF
				return nil
			})
		if err != nil {
			log.Println(err)
		}
	}
	close(jobs_chan)
}

func CheckForReference(cf transport.CliFile) (transport.CliFile){
	pbs:=GetPreviousBackups()
	c:=config.New()
	for _,pb := range(pbs.backaupInfos){
		cfOld := pb.DBS[c.TaskArgs.DBNow].Tables[c.TaskArgs.TableNow].Files[cf.Name]
		if len(cfOld.Reference) > 0 {
			continue
		}
		if cfOld.Sha1 == cf.Sha1{
			cf.Reference = pb.Name
			cf.Size = cfOld.Size
			cf.BSize = cfOld.BSize
			return cf
		}
	}
	return cf
}
/// This Job Running in Worker Pool
func BackupRun(cf transport.CliFile) (transport.CliFile, error) {
	c:=config.New()
	if c.TaskArgs.BackupType == "diff" ||
	   c.TaskArgs.BackupType == "incr" {
		err:=cf.Sha1Compute()
		if err!=nil{
			return cf,err
		}
		cf=CheckForReference(cf)
		if len(cf.Reference) > 0{
			return cf,nil
		}
	}
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
	cf.Size = tr.Size
	cf.BSize = tr.BSize
	return cf, nil
}

func Backup() error{
	// Main backup loop
	c:=config.New()
	ch:=database.New()
	ch.SetDSN(c.ClickhouseBackupConn)
	err:=CheckStorage()
	if err != nil{
		return err
	}
	backup_objects,err:= getBackupObjects()
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
	if c.TaskArgs.BackupType == "diff" ||
	   c.TaskArgs.BackupType == "incr" {
		pbs:=GetPreviousBackups()
		pbs.Search(c.TaskArgs.BackupType)
		print(len(pbs.backaupInfos))
	}
	for db,tables := range(backup_objects){
		c.TaskArgs.DBNow=db
		di:=database_info{
			Tables:    make(map[string]table_info),
			MetaData:  nil,
		}
		for _,table := range(tables){
			log.Printf("%s/%s",db,table)
			c.TaskArgs.TableNow=table
			var ti table_info
			if c.TaskArgs.BackupType=="part" {
				ti, _ = backupTable(db, table, c.TaskArgs.JobPartition)
			} else {
				ti, _ = backupTable(db, table, "")
			}
			di.Tables[table]=ti
			di.Add(&ti)
		}
		bi.DBS[db]=di
		bi.Add(&di)
		bi.StopDate=GetFormatedTime()
		BackupInfoWrite(&bi)
	}
	return nil
}

func backupMeta(db,table,fdb,ftable string) (transport.MetaFile,error){
	//mi := bi.DBS[db].MetaData[table]
	c:=config.New()
	ch:=database.New()
	mf:=transport.MetaFile{
		Name:     ftable+".sql",
		Path:     fdb,
		JobName:  c.TaskArgs.JobName,
		TryRetry: false,
	}

	meta,err:=ch.ShowCreateTable(db,table)
	if err != nil{
		return mf,err
	}
	mf.Content.WriteString(meta)
	err=transport.WriteMeta(&mf)
	return mf,err
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
		DbDir:      r[0],
		TableDir:   r[1],
		Partitions: parts,
		Files:      map[string]file_info{},
		BackupStatus: "bad",
	}
	mf,err:=backupMeta(db,table,ti.DbDir,ti.TableDir)
	if err == nil{
		ti.MetaData.Sha1=mf.Sha1
		ti.MetaData.Size=mf.Size
		ti.MetaData.BSize=mf.BSize
	}
	err=ch.FreezeTable(db,table,part)
	if err != nil{
		return table_info{BackupStatus: "bad"}, err
	}
	time.Sleep(time.Second*5) /// Clickhouse after freeze need some time
	c.ShadowDirIncr,err=ch.GetIncrement()
	if err != nil{
		return table_info{BackupStatus: "bad"}, err
	}
	defer RemoveShadowDirs()
	ti.Dirs=GetDirsInShadow(ti.DbDir,ti.TableDir)
	var wp_task workerpool.TaskFunc = func(i interface{}) (interface{}, error) {
		field, _ := i.(transport.CliFile)
		return BackupRun(field)
	}

	wp := workerpool.MakeWorkerPool(wp_task, 8, 3, 10)
	wp.Start()
	go FindFiles(wp.Get_Jobs_Chan())

	for job := range wp.Get_Results_Chan() {
		j, _ := job.(transport.CliFile)
		ti.AddJob(&j)
	}
	ti.BackupStatus="OK"
	return ti,nil
}

func getBackupObjects() (map[string][]string,error) {
	backup_objects:=map[string][]string{}
	c:=config.New()
	backup_filter:=c.BackupFilter
	ch:=database.New()
	C_DBS,err := ch.GetDBS()
	if err != nil {
		return nil, err
	}
	for _,db := range C_DBS{
		if db == "system" {
			continue
		}
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
	if c.TaskArgs.BackupType == "part"{
		if len(backup_filter) != 1 {
			return backup_filter,errors.New("Bad backup filter for parted mode, set only one db.table")
		}
		for _,tables := range(backup_filter){
			if len(tables) != 1 {
				return backup_filter,errors.New("Bad backup filter for parted mode, set only one db.table")
			}
		}
	}
	return backup_filter, nil
}

func BackupInfoWrite(bi *backup_info) (error) {
	c := config.New()
	byte, err := json.MarshalIndent(bi, "", "  ")
	if err != nil {
		if c.TaskArgs.Debug {
			log.Println("Marshal: %v", err)
		}
		return err
	}
	mf := transport.MetaFile{
		Name:     "backup.json",
		Path:     "",
		JobName:  c.TaskArgs.JobName,
		TryRetry: false,
		Sha1:     "",
	}
	for _,s := range ([]string{".copy",""}) {
		for {
			mf.Content.Write(byte)
			mf.Name = "backup.json" + s
			err = transport.WriteMeta(&mf)
			if err != nil {
				log.Println("Error write metafile ", mf.Path)
				continue
			}
			break
		}
	}
	return nil
}