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
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

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

func FindFiles(dir_for_backup string, jobs_chan chan<- workerpool.TaskElem) {
	err := filepath.Walk(dir_for_backup,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			cPath,err := SplitShadow(path)
			if err != nil{
				return nil
			}
			cliF := transport.CliFile{
				Name:       cPath[2],
				Path:       cPath[1],
				Shadow: 	cPath[0],
				RunJobType: transport.Backup,
				TryRetry:   false,
			}
			log.Printf("Backup  From %s Archive: %s",cliF.BackupSrcShort(),cliF.Archive())
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
	cf.Size = tr.Size
	cf.BSize = tr.BSize
	return cf, nil
}

func Backup() error{
	// Main backup loop
	c:=config.New()
	ch:=database.New()
	ch.SetDSN(c.ClickhouseBackupConn)
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
	for db,tables := range(backup_objects){
		di:=database_info{
			Tables:    make(map[string]table_info),
			MetaData:  nil,
		}
		for _,table := range(tables){
			log.Printf("%s/%s",db,table)
			ti,_:=backupTable(db,table,"")
			di.Tables[table]=ti
			di.Size+=ti.Size
			di.BSize+=ti.BSize
		}
		bi.DBS[db]=di
		bi.Size+=di.Size
		bi.BSize+=di.BSize
		bi.StopDate=GetFormatedTime()
		BackupWrite(&bi)
	}
	return nil
}

func backupMeta(db,table,fdb,ftable string) (transport.MetaFile,error){
	//mi := bi.DBS[db].MetaData[table]
	ch:=database.New()
	mf:=transport.MetaFile{
		Name:     ftable+".sql",
		Path:     fdb,
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
	shDir,err:=ch.GetIncrement()
	if err != nil{
		return table_info{BackupStatus: "bad"}, err
	}
	c.ShadowDir=path.Join(c.ClickhouseDir,"shadow",strconv.Itoa(shDir))
	defer os.RemoveAll(c.ShadowDir)
	ti.Dirs,_=GetDirs(path.Join(c.ShadowDir,"data",ti.DbDir,ti.TableDir))
	var wp_task workerpool.TaskFunc = func(i interface{}) (interface{}, error) {
		field, _ := i.(transport.CliFile)
		return BackupRun(field)
	}

	wp := workerpool.MakeWorkerPool(wp_task, 3, 3, 10)
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

func BackupWrite(bi *backup_info) (error) {
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
		TryRetry: false,
		Sha1:     "",
	}
	mf.Content.Write(byte)
	for _,s := range ([]string{".copy",""}) {
		for {
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