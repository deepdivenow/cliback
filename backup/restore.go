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
	"time"
)

func Restore() error{
	// Main restore loop
	//var restore_objects map[string][]string
	metas,err := transport.SearchMeta()
	if err != nil{
		return err
	}
	if len(metas) < 1{
		return errors.New("No backups for restore")
	}

	c:=config.New()
	backup_name := c.TaskArgs.JobName
	if len(c.TaskArgs.JobName) > 0 {
		if !Contains(metas,c.TaskArgs.JobName){
			return errors.New("Job #{c.TaskArgs.JobName} not exists for restore")
		}
	} else {
		log.Printf("Start Restore job: last")
		backup_name = metas[len(metas)-1]
		c.TaskArgs.JobName = backup_name
	}
	bi, err := BackupRead(backup_name)
	if err != nil{
		return err
	}
	log.Printf("Restore Job Name: %s", c.TaskArgs.JobName)
	ch:=database.New()
	ch.SetDSN(c.ClickhouseRestoreConn)
	defer ch.Close()
	switch bi.Version{
	case 1:
		return Restorev1(bi)
	case 2:
		return Restorev2(bi)
	default:
		return errors.New("Error read backup info version")
	}
}

func Restorev1(bi *backup_info) error{
	ch:=database.New()
	for db,db_info := range(bi.DBS) {
		ch.CreateDatabase(db)
		for table, table_info := range (db_info.Tables) {
			mi := bi.DBS[db].MetaData[table]
			mf:=transport.MetaFile{
				Name:     table_info.TableDir+".sql",
				Path:     table_info.DbDir,
				TryRetry: false,
				Sha1:     mi.Sha1,
			}
			meta,err:=transport.ReadMeta(mf)
			if err != nil{
				log.Println(err)
			}
			if mi.Sha1 != meta.Sha1{
				log.Printf("Backaup Info SHA1: %s not eq Restored file SHA1: %s",mi.Sha1,meta.Sha1)
			}
			err=ch.CreateTable(db,table,meta.Content.String())
			if err != nil{
				log.Println(err)
			}
			err=restore_table(&table_info)
			if err != nil{
				log.Println(err)
			}
			for _,part:=range(table_info.Partitions){
				err=ch.AttachPartition(db,table,part)
				if err!=nil{
					log.Printf("Error Attach partition `%s`.`%s`.%s",db,table,part)
				}
			}
		}
	}
	return nil
}

func Restorev2(bi *backup_info) error{
	return nil
}

func get_restore_objects() (map[string][]string,error) {
	var restore_objects map[string][]string


	return restore_objects, nil
}

func restore_table(ti *table_info) (error) {
	var wp_task workerpool.TaskFunc = func(i interface{}) (interface{}, error) {
		field, _ := i.(transport.CliFile)
		return RestoreRun(field)
	}
	wp := workerpool.MakeWorkerPool(wp_task, 4, 3, 10)
	wp.Start()
	go RestoreFiles(ti, wp.Get_Jobs_Chan())
	for job := range wp.Get_Results_Chan() {
		_, _ = job.(transport.CliFile)
	}
	return nil
}

func RestoreFiles(ti *table_info,jobs_chan chan<- workerpool.TaskElem) {
	for file,f_info := range (ti.Files){
		cliF := transport.CliFile{
			Name:       file,
			Path:       path.Join(ti.DbDir,ti.TableDir),
			RunJobType: transport.Restore,
			TryRetry:   false,
			Sha1: f_info.Sha1,
		}
		log.Printf("Restore archive: %s to %s",cliF.Archive(),cliF.RestoreDest())
		jobs_chan <- cliF
	}
	close(jobs_chan)
}

func RestoreRun(cf transport.CliFile) (transport.CliFile, error) {
	for {
		tr, err := transport.MakeTransport(cf)
		defer tr.Close()
		if err != nil {
			if err == os.ErrNotExist{
				log.Printf("%s %s",err,cf.Archive())
				return cf,err
			}
			log.Printf("Error open storage file %s, retry",cf.Archive())
			time.Sleep(time.Second*5)
			continue
		}
		_, err = tr.Copy()
		// Add copied check
		if err != nil {
			log.Printf("Error cp file %s,%s, retry",cf.Archive(),cf.RestoreDest())
			time.Sleep(time.Second*5)
			continue
		}
		restoredSha1 := hex.EncodeToString(tr.Sha1Sum.Sum(nil))
		if restoredSha1 != cf.Sha1 {
			log.Printf("File %s sha1 failed %s/%s", cf.RestoreDest(), cf.Sha1, restoredSha1)
		}
		return cf, nil
	}
}

func BackupRead(backup_name string) (*backup_info,error) {
	c := config.New()
	bi := new(backup_info)
	mf := transport.MetaFile{
		Name:     "backup.json",
		Path:     "",
		TryRetry: false,
		Sha1:     "",
	}
	mf,err:=transport.ReadMeta(mf)
	if err != nil{
		if c.TaskArgs.Debug {
			log.Println("Error read metafile ", mf.Path)
		}
		return nil, err
	}
	err = json.Unmarshal(mf.Content.Bytes(), bi)
	if err != nil {
		if c.TaskArgs.Debug {
			log.Println("Unmarshal: %v", err)
		}
		return nil, err
	}
	return bi,nil
}