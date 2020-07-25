package main

import (
	"cliback/backup"
	"cliback/config"
	"errors"
	"flag"
	"log"
)

type MainArgs struct{
	configFile  string
	backupMode  bool
	restoreMode bool
	infoMode    bool
	jobId          string
	partId         string
}

func (ma *MainArgs) parse_mode() error{
	var modeCount int = 0
	if ma.backupMode{ modeCount++ }
	if ma.restoreMode{ modeCount++ }
	if ma.infoMode{ modeCount++ }
	if modeCount == 1{
		return nil
	} else {
		return errors.New("Bad command line args usage: backup/restore/info")
	}
}


func main() {
	var cargs MainArgs
	flag.BoolVar(&cargs.restoreMode,"restore",false,"Run restore job")
	flag.BoolVar(&cargs.restoreMode,"r",false,"Run restore job (shotland)")
	flag.BoolVar(&cargs.backupMode,"backup",false,"Run backup job")
	flag.BoolVar(&cargs.backupMode,"b",false,"Run backup job (shotland)")
	flag.BoolVar(&cargs.infoMode,"info",false,"Get Info about backups")
	flag.BoolVar(&cargs.infoMode,"i",false,"Get Info about backups (shotland)")
	flag.BoolVar(&cargs.infoMode,"debug",false,"Debug messages")
	flag.BoolVar(&cargs.infoMode,"d",false,"Debug messages (shotland)")
	flag.StringVar(&cargs.configFile,"config","clickhouse_backup.yaml","path to config file")
	flag.StringVar(&cargs.configFile,"c","clickhouse_backup.yaml","path to config file (shotland)")
	flag.StringVar(&cargs.jobId,"jobid","","JobId for restore")
	flag.StringVar(&cargs.jobId,"j","","JobId for restore")
	flag.StringVar(&cargs.jobId,"partid","","PartId for backup OR restore")
	flag.StringVar(&cargs.jobId,"p","","PartId for backup OR restore")
	flag.Parse()

	err := cargs.parse_mode()
	if err != nil{
		println(err)
		flag.Usage()
		log.Fatalf("Exit by error on parse cmd args")
	}
	c := config.New()
	err=c.Read(cargs.configFile)
	if err != nil{
		println(err)
		flag.Usage()
		log.Fatalf("Please check config file")
	}

	c.TaskArgs.JobName=cargs.jobId
	c.TaskArgs.JobPartition=cargs.partId

	if cargs.infoMode {
		c.TaskArgs.JobType=config.Info
		err=backup.Info()
	} else if cargs.backupMode {
		c.TaskArgs.JobType=config.Backup
		err=backup.Backup()
	} else if cargs.restoreMode {
		c.TaskArgs.JobType=config.Restore
		err=backup.Restore()
	} else {
		log.Fatalf("Bad programm Running mode")
	}
	if err != nil{
		log.Fatal(err)
	}

	//println("hello")
	//
	//a,_:=transport.SearchMeta()
	//println(a)
	//c.BackupStorage.BackupDir=path.Join(c.BackupStorage.BackupDir,"20200327_065237P")
	//c.SetShadow("/home/dro/.thunderbird")
	//backup.Restore()
	log.Println("Exit")
}
