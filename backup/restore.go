package backup

import (
	"cliback/config"
	"cliback/database"
	"cliback/status"
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

func GetMetaForRestore() (*backupInfo, error) {
	var bi *backupInfo
	metas, err := transport.SearchMeta()
	if err != nil {
		return bi, err
	}
	if len(metas) < 1 {
		return bi, errors.New("No backups for restore")
	}
	c := config.New()
	backupName := c.TaskArgs.JobName
	if len(c.TaskArgs.JobName) > 0 {
		if !Contains(metas, c.TaskArgs.JobName) {
			return bi, errors.New("Job #{c.TaskArgs.JobName} not exists for restore")
		}
		return BackupRead(backupName)
	} else {
		log.Printf("Start Restore job: `Last`")
		for i := len(metas) - 1; i >= 0; i-- {
			backupName = metas[i]
			c.TaskArgs.JobName = backupName
			log.Printf("Try read meta for backup: %s", backupName)
			bi, err = BackupRead(backupName)
			if err != nil {
				log.Printf("Read meta for backup: %s, Fail %s", backupName, err)
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
		s := status.New()
		s.SetStatus(status.FailRestoreMeta)
		return err
	}
	log.Printf("Restore Job Name: %s", c.TaskArgs.JobName)
	ch := database.New()
	ch.SetDSN(c.ClickhouseRestoreConn)
	ch.SetMetaOpts(c.ClickhouseRestoreOpts)
	defer ch.Close()
	err = CheckStorage()
	if err != nil {
		s := status.New()
		s.SetStatus(status.FailClickhouseStorage)
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

func Restorev1(bi *backupInfo) error {
	ch := database.New()
	c := config.New()
	for db, dbInfo := range bi.DBS {
		err := ch.CreateDatabase(db)
		if err != nil {
			s := status.New()
			s.SetStatus(status.FailRestoreDatabase)
			log.Printf("Create database error: %v", err)
		}
		for table, tableInfo := range dbInfo.Tables {
			if len(tableInfo.DbDir) < 1 {
				tableInfo.DbDir = db
			}
			if len(tableInfo.TableDir) < 1 {
				tableInfo.TableDir = table
			}
			mi := bi.DBS[db].Tables[table].MetaData
			mf := transport.MetaFile{
				Name:     tableInfo.TableDir + ".sql",
				Path:     tableInfo.DbDir,
				JobName:  c.TaskArgs.JobName,
				TryRetry: false,
				Sha1:     mi.Sha1,
			}
			err := transport.ReadMeta(&mf)
			if err != nil {
				s := status.New()
				s.SetStatus(status.FailRestoreMeta)
				log.Println(err)
			}
			if mi.Sha1 != mf.Sha1 {
				s := status.New()
				s.SetStatus(status.FailRestoreMeta)
				log.Printf("Backup Info SHA1: %s not eq Restored file SHA1: %s", mi.Sha1, mf.Sha1)
			}
			err = ch.CreateTable(db, table, mf.Content.String())
			if err != nil {
				s := status.New()
				s.SetStatus(status.FailRestoreTable)
				log.Println(err)
			}
			err = restoreTable(&tableInfo)
			if err != nil {
				s := status.New()
				s.SetStatus(status.FailRestoreTable)
				log.Println(err)
			}
			if len(tableInfo.Partitions) == 1 && tableInfo.Partitions[0] == "tuple()" {
				for _, dir := range tableInfo.Dirs {
					err = ch.AttachPartitionByDir(db, table, dir)
					if err != nil {
						s := status.New()
						s.SetStatus(status.FailRestorePartition)
						log.Printf("Error Attach dir `%s`.`%s`.%s", db, table, dir)
					}
				}
			} else {
				for _, part := range tableInfo.Partitions {
					err = ch.AttachPartition(db, table, part)
					if err != nil {
						s := status.New()
						s.SetStatus(status.FailRestorePartition)
						log.Printf("Error Attach partition `%s`.`%s`.%s", db, table, part)
					}
				}
			}
		}
	}
	return nil
}

func Restorev2(bi *backupInfo) error {
	return nil
}

func getRestoreObjects() (map[string][]string, error) {
	var restoreObjects map[string][]string

	return restoreObjects, nil
}

func restoreTable(ti *tableInfo) error {
	var wpTask workerpool.TaskFunc = func(i interface{}) (interface{}, error) {
		field, _ := i.(transport.CliFile)
		return RestoreRun(field)
	}
	wp := workerpool.MakeWorkerPool(wpTask, 8, 3, 10)
	wp.Start()
	go RestoreFiles(ti, wp.GetJobsChan())
	for job := range wp.GetResultsChan() {
		_, _ = job.(transport.CliFile)
	}
	return nil
}

func RestoreFiles(ti *tableInfo, jobsChan chan<- workerpool.TaskElem) {
	for file, fileInfo := range ti.Files {
		cliF := transport.CliFile{
			Name:       file,
			Path:       path.Join(ti.DbDir, ti.TableDir),
			RunJobType: transport.Restore,
			TryRetry:   false,
			Sha1:       fileInfo.Sha1,
			Size:       fileInfo.Size,
			BSize:      fileInfo.BSize,
			Reference:  fileInfo.Reference,
			Storage:    fileInfo.Storage,
		}
		log.Printf("Restore archive: %s to %s", cliF.Archive(), cliF.RestoreDest())
		jobsChan <- cliF
	}
	close(jobsChan)
}

func RestoreRun(cf transport.CliFile) (transport.CliFile, error) {
	for {
		tr, err := transport.MakeTransport(cf)
		if err != nil {
			if err == os.ErrNotExist {
				log.Printf("File not Exists %s %s", err, cf.Archive())
				s := status.New()
				s.SetStatus(status.FailRestoreFile)
				return cf, err
			}
			log.Printf("Error open storage file %s Retry. Err: %v", cf.Archive(), err)
			time.Sleep(time.Second * 5)
			continue
		}
		defer tr.Close()
		_, err = tr.Copy()
		// Add copied check
		if err != nil {
			log.Printf("Error cp file %s,%s, retry", cf.Archive(), cf.RestoreDest())
			time.Sleep(time.Second * 5)
			tr.Close()
			continue
		}
		restoredSha1 := hex.EncodeToString(tr.Sha1Sum.Sum(nil))
		if restoredSha1 != cf.Sha1 {
			s := status.New()
			s.SetStatus(status.FailRestoreFile)
			log.Printf("File %s sha1 failed %s/%s", cf.RestoreDest(), cf.Sha1, restoredSha1)
		}
		return cf, nil
	}
}

func BackupRead(backupName string) (*backupInfo, error) {
	c := config.New()
	bi := new(backupInfo)
	mf := transport.MetaFile{
		Name:     "backup.json",
		Path:     "",
		JobName:  backupName,
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
			log.Printf("Unmarshal: %v", err)
		}
		return nil, err
	}
	return bi, nil
}
