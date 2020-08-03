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

func GetMetaForRestore() (*backup_info, error) {
	var bi *backup_info
	metas, err := transport.SearchMeta()
	if err != nil {
		return bi, err
	}
	if len(metas) < 1 {
		return bi, errors.New("No backups for restore")
	}
	c := config.New()
	backup_name := c.TaskArgs.JobName
	if len(c.TaskArgs.JobName) > 0 {
		if !Contains(metas, c.TaskArgs.JobName) {
			return bi, errors.New("Job #{c.TaskArgs.JobName} not exists for restore")
		}
		return BackupRead(backup_name)
	} else {
		log.Printf("Start Restore job: `Last`")
		for i := len(metas) - 1; i >= 0; i-- {
			backup_name = metas[i]
			c.TaskArgs.JobName = backup_name
			log.Printf("Try read meta for backup: %s", backup_name)
			bi, err = BackupRead(backup_name)
			if err != nil {
				log.Printf("Read meta for backup: %s, Fail %s", backup_name, err)
				continue
			}
			return bi, nil
		}
	}
	return bi, errors.New("No backups for restore")
}

func Restore() error {
	c := config.New()
	bi, err := GetMetaForRestore()
	if err != nil {
		return err
	}
	log.Printf("Restore Job Name: %s", c.TaskArgs.JobName)
	ch := database.New()
	ch.SetDSN(c.ClickhouseRestoreConn)
	ch.SetMetaOpts(c.ClickhouseRestoreOpts)
	defer ch.Close()
	err = CheckStorage()
	if err != nil {
		return err
	}
	switch bi.Version {
	case 1:
		return Restorev1(bi)
	case 2:
		return Restorev2(bi)
	default:
		return errors.New("Error read backup info version")
	}
}

func Restorev1(bi *backup_info) error {
	ch := database.New()
	c := config.New()
	for db, db_info := range bi.DBS {
		ch.CreateDatabase(db)
		for table, table_info := range db_info.Tables {
			if len(table_info.DbDir) < 1 {
				table_info.DbDir = db
			}
			if len(table_info.TableDir) < 1 {
				table_info.TableDir = table
			}
			mi := bi.DBS[db].MetaData[table]
			mf := transport.MetaFile{
				Name:     table_info.TableDir + ".sql",
				Path:     table_info.DbDir,
				JobName:  c.TaskArgs.JobName,
				TryRetry: false,
				Sha1:     mi.Sha1,
			}
			err := transport.ReadMeta(&mf)
			if err != nil {
				log.Println(err)
			}
			if mi.Sha1 != mf.Sha1 {
				log.Printf("Backup Info SHA1: %s not eq Restored file SHA1: %s", mi.Sha1, mf.Sha1)
			}
			err = ch.CreateTable(db, table, mf.Content.String())
			if err != nil {
				log.Println(err)
			}
			err = restoreTable(&table_info)
			if err != nil {
				log.Println(err)
			}
			if len(table_info.Partitions) == 1 && table_info.Partitions[0] == "tuple()" {
				for _, dir := range table_info.Dirs {
					err = ch.AttachPartitionByDir(db, table, dir)
					if err != nil {
						log.Printf("Error Attach dir `%s`.`%s`.%s", db, table, dir)
					}
				}
			} else {
				for _, part := range table_info.Partitions {
					err = ch.AttachPartition(db, table, part)
					if err != nil {
						log.Printf("Error Attach partition `%s`.`%s`.%s", db, table, part)
					}
				}
			}
		}
	}
	return nil
}

func Restorev2(bi *backup_info) error {
	return nil
}

func get_restore_objects() (map[string][]string, error) {
	var restore_objects map[string][]string

	return restore_objects, nil
}

func restoreTable(ti *table_info) error {
	var wp_task workerpool.TaskFunc = func(i interface{}) (interface{}, error) {
		field, _ := i.(transport.CliFile)
		return RestoreRun(field)
	}
	wp := workerpool.MakeWorkerPool(wp_task, 8, 3, 10)
	wp.Start()
	go RestoreFiles(ti, wp.Get_Jobs_Chan())
	for job := range wp.Get_Results_Chan() {
		_, _ = job.(transport.CliFile)
	}
	return nil
}

func RestoreFiles(ti *table_info, jobs_chan chan<- workerpool.TaskElem) {
	for file, f_info := range ti.Files {
		cliF := transport.CliFile{
			Name:       file,
			Path:       path.Join(ti.DbDir, ti.TableDir),
			RunJobType: transport.Restore,
			TryRetry:   false,
			Sha1:       f_info.Sha1,
			Size:       f_info.Size,
			BSize:      f_info.BSize,
			Reference:  f_info.Reference,
			Storage:    f_info.Storage,
		}
		log.Printf("Restore archive: %s to %s", cliF.Archive(), cliF.RestoreDest())
		jobs_chan <- cliF
	}
	close(jobs_chan)
}

func RestoreRun(cf transport.CliFile) (transport.CliFile, error) {
	for {
		tr, err := transport.MakeTransport(cf)
		defer tr.Close()
		if err != nil {
			if err == os.ErrNotExist {
				log.Printf("%s %s", err, cf.Archive())
				return cf, err
			}
			log.Printf("Error open storage file %s, retry", cf.Archive())
			time.Sleep(time.Second * 5)
			continue
		}
		_, err = tr.Copy()
		// Add copied check
		if err != nil {
			log.Printf("Error cp file %s,%s, retry", cf.Archive(), cf.RestoreDest())
			time.Sleep(time.Second * 5)
			continue
		}
		restoredSha1 := hex.EncodeToString(tr.Sha1Sum.Sum(nil))
		if restoredSha1 != cf.Sha1 {
			log.Printf("File %s sha1 failed %s/%s", cf.RestoreDest(), cf.Sha1, restoredSha1)
		}
		return cf, nil
	}
}

func BackupRead(backup_name string) (*backup_info, error) {
	c := config.New()
	bi := new(backup_info)
	mf := transport.MetaFile{
		Name:     "backup.json",
		Path:     "",
		JobName:  backup_name,
		TryRetry: false,
		Sha1:     "",
	}
	err := transport.ReadMeta(&mf)
	if err != nil {
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
	return bi, nil
}
