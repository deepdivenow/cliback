package backup

import (
	"cliback/config"
	"cliback/database"
	"cliback/status"
	"cliback/transport"
	"cliback/workerpool"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"time"
)

func FindFiles(jobsChan chan<- workerpool.TaskElem, tInfo database.TableInfo) {
	c := config.New()
	for storage := range c.ClickhouseStorage {
		dirForBackup := c.GetShadow(storage)
		st, err := os.Stat(dirForBackup)
		if err != nil {
			continue
		}
		if !st.IsDir() {
			continue
		}
		err = filepath.Walk(dirForBackup,
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
					DBName:     tInfo.GetDBNameE(),
					TableName:  tInfo.GetTableNameE(),
					RunJobType: transport.Backup,
					TryRetry:   false,
					Storage:    storage,
				}
				log.Printf("Backup  From %s Archive: %s", cliF.BackupSrcShort(), cliF.Archive())
				jobsChan <- cliF
				return nil
			})
		if err != nil {
			log.Println(err)
			s := status.New()
			s.SetStatus(status.FailBackupTable)
		}
	}
	close(jobsChan)
}

func CheckForReference(cf transport.CliFile) transport.CliFile {
	pbs := GetPreviousBackups()
	c := config.New()
	for _, pb := range pbs.backupInfos {
		cfOld := pb.DBS[c.TaskArgs.DBNow].Tables[c.TaskArgs.TableNow].Files[cf.Name]
		if len(cfOld.Reference) > 0 {
			continue
		}
		if cfOld.Sha1 == cf.Sha1 {
			cf.Reference = pb.Name
			cf.Size = cfOld.Size
			cf.BSize = cfOld.BSize
			return cf
		}
	}
	return cf
}

// BackupRun This func Running in Worker Pool
func BackupRun(cf transport.CliFile) (transport.CliFile, error) {
	c := config.New()
	for {
		if c.TaskArgs.BackupType == "diff" ||
			c.TaskArgs.BackupType == "incr" {
			err := cf.Sha1Compute()
			if err != nil {
				if err == os.ErrNotExist {
					log.Printf("File not Exists %s %s", err, cf.BackupSrc())
					s := status.New()
					s.SetStatus(status.FailBackupFile)
					return cf, err
				}
				log.Printf("Error open shadow file %s Retry. Err: %v", cf.BackupSrc(), err)
				time.Sleep(time.Second * 5)
				continue
			}
			cf = CheckForReference(cf)
			if len(cf.Reference) > 0 {
				return cf, nil
			}
		}
		tr, err := transport.MakeTransport()
		if err != nil {
			return cf, err
		}
		trStat, err := tr.Do(cf)
		if err != nil {
			if err == os.ErrNotExist {
				log.Printf("File not Exists %s %s", err, cf.BackupSrc())
				s := status.New()
				s.SetStatus(status.FailBackupFile)
				return cf, err
			}
			log.Printf("Error open shadow file %s Retry. Err: %v", cf.BackupSrc(), err)
			time.Sleep(time.Second * 5)
			continue
		}
		cf.Sha1 = trStat.Sha1Sum
		cf.Size = trStat.Size
		cf.BSize = trStat.BSize
		return cf, nil
	}
}

func Backup() error {
	// Main backup loop
	retentionBeforeBackup()
	c := config.New()
	ch := database.New()
	ch.SetDSN(c.ClickhouseBackupConn)
	err := CheckStorage()
	if err != nil {
		return err
	}
	backupObjects, err := getBackupObjects()
	if err != nil {
		return err
	}

	if len(c.TaskArgs.JobName) < 1 {
		c.TaskArgs.JobName = GenerateBackupName()
	}
	log.Printf("Backup Job Name: %s", c.TaskArgs.JobName)

	bi := backupInfo{
		Name:         c.TaskArgs.JobName,
		Type:         c.TaskArgs.BackupType,
		Version:      1,
		BackupFilter: backupObjects,
		StartDate:    GetFormatedTime(),
		DBS:          map[string]databaseInfo{},
	}
	if c.TaskArgs.BackupType == "diff" ||
		c.TaskArgs.BackupType == "incr" {
		pbs := GetPreviousBackups()
		err := pbs.Search(c.TaskArgs.BackupType)
		if err != nil {
			return err
		}
		log.Printf("Search delta by backups: %s", pbs.GetBackupNames())
	}
	for db, tables := range backupObjects {
		c.TaskArgs.DBNow = db
		di := databaseInfo{
			Tables:   map[string]tableInfo{},
			MetaData: map[string]fileInfo{},
		}
		for _, table := range tables {
			log.Printf("Backup table: `%s`.`%s`", db, table)
			c.TaskArgs.TableNow = table
			var ti tableInfo
			if c.TaskArgs.BackupType == "part" {
				ti, _ = backupTable(db, table, c.TaskArgs.JobPartition)
			} else {
				ti, _ = backupTable(db, table, "")
			}
			di.Tables[table] = ti
			// Added for backward compatibility
			di.MetaData[table] = ti.MetaData
			di.Add(&ti)
		}
		bi.DBS[db] = di
		bi.Add(&di)
		bi.StopDate = GetFormatedTime()
		err = BackupInfoWrite(&bi)
		if err != nil {
			log.Printf("Write backup info error: %v", err)
		}
	}
	log.Print("Backup info:\n" + bi.String())
	return nil
}

func backupMeta(tInfo database.TableInfo) (transport.MetaFile, error) {
	//mi := bi.DBS[db].MetaData[table]
	c := config.New()
	ch := database.New()
	mf := transport.MetaFile{
		Name:     tInfo.TableName + ".sql",
		Path:     tInfo.DBName,
		JobName:  c.TaskArgs.JobName,
		TryRetry: false,
	}

	meta, err := ch.ShowCreateTable(tInfo.DBName, tInfo.TableName)
	if err != nil {
		return mf, err
	}
	mf.Content.WriteString(meta)
	tr, err := transport.MakeTransport()
	if err != nil {
		return mf, err
	}
	err = tr.WriteMeta(&mf)
	return mf, err
}

func backupTable(db, table, part string) (tableInfo, error) {
	c := config.New()
	ch := database.New()
	parts, err := ch.GetPartitions(db, table, part)
	if err != nil {
		return tableInfo{BackupStatus: "bad"}, err
	}
	tInfo, err := ch.GetTableInfo(db, table)
	if err != nil {
		return tableInfo{BackupStatus: "bad"}, err
	}
	ti := tableInfo{
		DbDir:        tInfo.GetDBNameE(),
		TableDir:     tInfo.GetTableNameE(),
		Partitions:   parts,
		Files:        map[string]fileInfo{},
		BackupStatus: "bad",
	}
	mf, err := backupMeta(tInfo)
	if err == nil {
		ti.MetaData.Sha1 = mf.Sha1
		ti.MetaData.Size = mf.Size
		ti.MetaData.BSize = mf.BSize
	} else {
		s := status.New()
		s.SetStatus(status.FailBackupMeta)
	}
	err = ch.FreezeTable(db, table, part)
	if err != nil {
		s := status.New()
		s.SetStatus(status.FailFreezeTable)
		return tableInfo{BackupStatus: "bad"}, err
	}
	time.Sleep(time.Second * 1) /// Clickhouse after freeze need some time
	c.ShadowDirIncr, err = ch.GetIncrement()
	if err != nil {
		s := status.New()
		s.SetStatus(status.FailGetIncrement)
		return tableInfo{BackupStatus: "bad"}, err
	}
	defer RemoveShadowDirs()
	ti.Dirs = GetDirsInShadow(tInfo)
	var wpTask workerpool.TaskFunc = func(i interface{}) (interface{}, error) {
		field, _ := i.(transport.CliFile)
		return BackupRun(field)
	}

	wp := workerpool.MakeWorkerPool(wpTask, c.WorkerPool.NumWorkers, c.WorkerPool.NumRetry, c.WorkerPool.ChanLen)
	wp.Start()
	go FindFiles(wp.GetJobsChan(), tInfo)

	for job := range wp.GetResultsChan() {
		j, _ := job.(transport.CliFile)
		ti.AddJob(&j)
	}
	ti.BackupStatus = "OK"
	return ti, nil
}

func getBackupObjects() (map[string][]string, error) {
	backupObjects := map[string][]string{}
	c := config.New()
	backupFilter := c.BackupFilter
	ch := database.New()
	currentDBS, err := ch.GetDBS()
	if err != nil {
		s := status.New()
		s.SetStatus(status.FailGetDBS)
		return nil, err
	}
	for _, db := range currentDBS {
		if db == "system" {
			continue
		}
		currentTables, err := ch.GetTables(db)
		if err != nil {
			s := status.New()
			s.SetStatus(status.FailGetTables)
			return nil, err
		}
		//clone slice
		backupObjects[db] = append(currentTables[:0:0], currentTables...)
	}
	if backupFilter == nil {
		return backupObjects, nil
	}
	for db, tables := range backupFilter {
		if _, ok := backupObjects[db]; !ok {
			return nil, errors.New("Bad filter, not contains in database")
		}
		if tables == nil {
			backupFilter[db] = backupObjects[db]
			continue
		}
		for _, table := range tables {
			if !Contains(backupObjects[db], table) {
				return nil, errors.New("Bad filter, not contains in database")
			}
		}
	}
	if c.TaskArgs.BackupType == "part" {
		if len(backupFilter) != 1 {
			return backupFilter, errors.New("Bad backup filter for parted mode, set only one db.table")
		}
		for _, tables := range backupFilter {
			if len(tables) != 1 {
				return backupFilter, errors.New("Bad backup filter for parted mode, set only one db.table")
			}
		}
	}
	return backupFilter, nil
}

func BackupInfoWrite(bi *backupInfo) error {
	c := config.New()
	prepareBytes, err := json.MarshalIndent(bi, "", "  ")
	if err != nil {
		if c.TaskArgs.Debug {
			log.Printf("Marshal: %v", err)
		}
		s := status.New()
		s.SetStatus(status.FailBackupMeta)
		return err
	}
	mf := transport.MetaFile{
		Name:     "backup.json",
		Path:     "",
		JobName:  c.TaskArgs.JobName,
		TryRetry: false,
		Sha1:     "",
	}
	for _, s := range []string{".copy", ""} {
		for {
			mf.Content.Write(prepareBytes)
			mf.Name = "backup.json" + s
			tr, err := transport.MakeTransport()
			if err != nil {
				return err
			}
			err = tr.WriteMeta(&mf)
			if err != nil {
				log.Println("Error write metafile ", mf.Path)
				time.Sleep(time.Second * 5)
				continue
			}
			break
		}
	}
	return nil
}
