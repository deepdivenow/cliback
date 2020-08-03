package backup

import (
	"cliback/transport"
	"errors"
	"regexp"
	"sync"
)

type previous_backups struct {
	backaupInfos []*backup_info
	founded bool
}

var (
	once sync.Once
	instance *previous_backups
)

func GetPreviousBackups() *previous_backups {
	once.Do(func() {
		instance = new(previous_backups)
	})
	return instance
}

func (pb *previous_backups) Founded() (bool){
	return pb.founded
}

func (pb *previous_backups) Search(t string) error{
	metas,err := transport.SearchMeta()
	if err != nil{
		return err
	}
	var result_chain []*backup_info
	// Search Full Backup
	var fullBackupPos int
	for i:=len(metas)-1; i>0; i-- {
		if re_match, _ := regexp.MatchString("^(\\d{8}_\\d{6}[F]{1})$", metas[i]); re_match {
			meta,err:=BackupRead(metas[i])
			if err != nil{
				continue
			}
			result_chain=append(result_chain, meta)
			fullBackupPos=i
			break
		}
	}
	if len(result_chain) <1 {
		return errors.New("Previous backups not found")
	}
	if t == "diff"{
		pb.backaupInfos=result_chain
	}
	for i:=fullBackupPos+1; i<len(metas); i++ {
		if re_match, _ := regexp.MatchString("^(\\d{8}_\\d{6}[F]{1})$", metas[i]); re_match {
			break
		}
		if re_match, _ := regexp.MatchString("^(\\d{8}_\\d{6}[DI]{1})$", metas[i]); re_match {
			meta,err:=BackupRead(metas[i])
			if err != nil{
				continue
			}
			result_chain=append(result_chain, meta)
		}
	}
	pb.backaupInfos=result_chain
	return nil
}

