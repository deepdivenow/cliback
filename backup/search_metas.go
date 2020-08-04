package backup

import (
	"cliback/transport"
	"errors"
	"regexp"
	"sort"
	"sync"
)

type previousBackups struct {
	backupInfos []*backupInfo
	founded     bool
}

var (
	once     sync.Once
	instance *previousBackups
)

func GetPreviousBackups() *previousBackups {
	once.Do(func() {
		instance = new(previousBackups)
	})
	return instance
}

func (pb *previousBackups) Founded() bool {
	return pb.founded
}

func (pb *previousBackups) Search(t string) error {
	metas, err := transport.SearchMeta()
	if err != nil {
		return err
	}
	var resultChain []*backupInfo
	// Search Full Backup
	var fullBackupPos int
	for i := len(metas) - 1; i > 0; i-- {
		if reMatch, _ := regexp.MatchString("^(\\d{8}_\\d{6}[F]{1})$", metas[i]); reMatch {
			meta, err := BackupRead(metas[i])
			if err != nil {
				continue
			}
			resultChain = append(resultChain, meta)
			fullBackupPos = i
			break
		}
	}
	if len(resultChain) < 1 {
		pb.founded = false
		return errors.New("Previous backups not found")
	}
	pb.founded = true
	if t == "diff" {
		pb.backupInfos = resultChain
	}
	for i := fullBackupPos + 1; i < len(metas); i++ {
		if reMatch, _ := regexp.MatchString("^(\\d{8}_\\d{6}[F]{1})$", metas[i]); reMatch {
			break
		}
		if reMatch, _ := regexp.MatchString("^(\\d{8}_\\d{6}[DI]{1})$", metas[i]); reMatch {
			meta, err := BackupRead(metas[i])
			if err != nil {
				continue
			}
			resultChain = append(resultChain, meta)
		}
	}
	pb.backupInfos = resultChain
	return nil
}

func (pb *previousBackups) GetBackupNames() []string {
	var result []string
	for _, bi := range pb.backupInfos {
		result = append(result, bi.Name)
	}
	sort.Strings(result)
	return result
}
