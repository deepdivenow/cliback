package main

import (
	"cliback/backup"
	"cliback/config"
	"cliback/transport"
	"path"
)


func main() {
	c := config.New()
	c.Read("/home/dro/go-1.13/src/cliback/clickhouse_backup.yaml")
	a,_:=transport.SearchMeta()
	println(a)
	c.BackupStorage.BackupDir=path.Join(c.BackupStorage.BackupDir,"20200327_065237P")
	c.SetShadow("/home/dro/.thunderbird")
	backup.Restore()
}
