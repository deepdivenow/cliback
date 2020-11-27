package backup

import (
	"cliback/backupMap"
	"cliback/config"
	"cliback/transport"
	"log"
	"os"
)

func retentionBeforeBackup() error {
	return retentionCleanup()
}

func retentionAfterBackup() error {
	return nil
}

func retentionCleanup() error {
	c:=config.New()
	if c.RetentionBackupFull < 1 {
		return nil
	}
	log.Println("Retention: Start...")
	log.Println("Retention: RetentionBackupFull: ", c.RetentionBackupFull)
	bm := backupMap.New()
	metas, err := transport.SearchMeta()
	if err != nil {
		return err
	}
	if len(metas) < 1 {
		return nil
	}
	// Check backups state, create map
	var badBackups []string
	for _, backupName := range metas {
		bi, err := BackupRead(backupName)
		if err != nil {
			log.Println("Retention: ",backupName,err)
			if err == os.ErrNotExist {
				log.Println("Retention: ",backupName,"Added to BadBackups")
				badBackups = append(badBackups, backupName)
			}
			continue
		}
		bm.Add(bi.Name, bi.Reference...)
	}
	log.Println("Retention: Walk storage ends")
	log.Println("Retention: Bad backups:", badBackups)
	// log.Println("Retention: Deps Forward:", bm.GetDepsForward())
	// log.Println("Retention: Deps Backward:", bm.GetDepsBackward())
	log.Println("Retention: Bad deps:", bm.GetBadDeps())
	log.Println("Retention: Fulls for Delete:", bm.GetFullsForDelete(c.RetentionBackupFull))
	log.Println("Retention: Backups for Delete:", bm.GetBackupsForDelete(c.RetentionBackupFull))
	log.Println("Retention: Fulls for Store:", bm.GetFullsForStore(c.RetentionBackupFull))
	log.Println("Retention: Backups for Store:", bm.GetBackupsForStore(c.RetentionBackupFull))
	retentionDeleteBackup(badBackups)
	retentionDeleteBackup(bm.GetBadDeps())
	retentionDeleteBackup(bm.GetBackupsForDelete(c.RetentionBackupFull))
	log.Println("Retention: Finish")
	return nil
}

func retentionDeleteBackup(backups []string) error {
	for _,b := range backups {
		log.Println("Retention: BackupDelete ",b)
		err:=transport.DeleteBackup(b)
		if err != nil {
			log.Println("Retention: BackupDelete ",b,err)
		}
	}
	return nil
}