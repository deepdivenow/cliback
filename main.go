package main

import (
	"cliback/backup"
	"cliback/config"
	"cliback/status"
	"errors"
	"flag"
	"log"
	"os"
)

type MainArgs struct {
	configFile  string
	backupMode  bool
	restoreMode bool
	infoMode    bool
	jobId       string
	partId      string
	backupType  string
}

func (ma *MainArgs) parse_mode() error {
	var modeCount int = 0
	if ma.backupMode {
		modeCount++
	}
	if ma.restoreMode {
		modeCount++
	}
	if ma.infoMode {
		modeCount++
	}
	if modeCount == 1 {
		return nil
	} else {
		return errors.New("Bad command line args usage: backup/restore/info")
	}
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

func main() {
	var cargs MainArgs
	flag.BoolVar(&cargs.restoreMode, "restore", false, "Run restore job")
	flag.BoolVar(&cargs.restoreMode, "r", false, "Run restore job (shotland)")
	flag.BoolVar(&cargs.backupMode, "backup", false, "Run backup job")
	flag.BoolVar(&cargs.backupMode, "b", false, "Run backup job (shotland)")
	flag.BoolVar(&cargs.infoMode, "info", false, "Get Info about backups")
	flag.BoolVar(&cargs.infoMode, "i", false, "Get Info about backups (shotland)")
	flag.BoolVar(&cargs.infoMode, "debug", false, "Debug messages")
	flag.BoolVar(&cargs.infoMode, "d", false, "Debug messages (shotland)")
	flag.StringVar(&cargs.configFile, "config", "clickhouse_backup.yaml", "path to config file")
	flag.StringVar(&cargs.configFile, "c", "clickhouse_backup.yaml", "path to config file (shotland)")
	flag.StringVar(&cargs.jobId, "jobid", "", "JobId for restore")
	flag.StringVar(&cargs.jobId, "j", "", "JobId for restore (shotland)")
	flag.StringVar(&cargs.backupType, "backup-type", "", "Backup type (default: full)")
	flag.StringVar(&cargs.backupType, "t", "", "Backup type (default: full) (shotland)")
	flag.StringVar(&cargs.partId, "partid", "", "PartId for backup OR restore ")
	flag.StringVar(&cargs.partId, "p", "", "PartId for backup OR restore (shotland)")
	flag.Parse()

	err := cargs.parse_mode()
	if err != nil {
		println(err)
		flag.Usage()
		log.Fatalf("Exit by error on parse cmd args")
	}
	s := status.New()
	c := config.New()
	err = c.Read(cargs.configFile)
	if err != nil {
		s.SetStatus(status.FailReadConfig)
		println(err)
		flag.Usage()
		log.Println("Please check config file")
		os.Exit(s.GetFinalStatus())
	}

	c.TaskArgs.JobName = cargs.jobId
	c.TaskArgs.JobPartition = cargs.partId
	if len(cargs.backupType) > 0 && Contains([]string{"full", "diff", "incr", "part"}, cargs.backupType) {
		c.TaskArgs.BackupType = cargs.backupType
	} else {
		c.TaskArgs.BackupType = "full"
	}
	if cargs.infoMode {
		c.TaskArgs.JobType = config.Info
		err = backup.Info()
		if err != nil {
			s.SetStatus(status.FailInfo)
		}
	} else if cargs.backupMode {
		c.TaskArgs.JobType = config.Backup
		err = backup.Backup()
		if err != nil {
			s.SetStatus(status.FailBackup)
		}
	} else if cargs.restoreMode {
		c.TaskArgs.JobType = config.Restore
		err = backup.Restore()
		if err != nil {
			s.SetStatus(status.FailRestore)
		}
	} else {
		log.Fatalf("Bad programm Running mode")
	}
	if err != nil {
		log.Println(err)
	}
	log.Printf("Exit %d", s.GetFinalStatus())
	os.Exit(s.GetFinalStatus())
}
