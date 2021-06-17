package backupMap

import (
	"fmt"
	"regexp"
	"sort"
	"sync"
)

var (
	once       sync.Once
	iBackupMap *backupMap
)

type backupMap struct {
	depsForward   map[string][]string
	depsBackward  map[string][]string
	backupsExists []string
}

func (bm *backupMap) GetDepsForward() map[string][]string {
	return bm.depsForward
}

func (bm *backupMap) GetDepsBackward() map[string][]string {
	return bm.depsBackward
}

func New() *backupMap {
	once.Do(func() {
		iBackupMap = new(backupMap)
		iBackupMap.depsForward = make(map[string][]string)
		iBackupMap.depsBackward = make(map[string][]string)
	})
	return iBackupMap
}

func (bm *backupMap) Add(backupName string, dependedFrom ...string) {
	if !Contains(bm.backupsExists, backupName) {
		bm.backupsExists = append(bm.backupsExists, backupName)
	}
	for _, dep := range dependedFrom {
		if !metaDirNameMatched(dep) {
			continue
		}
		if !Contains(bm.depsForward[dep], backupName) {
			bm.depsForward[dep] = append(bm.depsForward[dep], backupName)
		}
		if !Contains(bm.depsBackward[backupName], dep) {
			bm.depsBackward[backupName] = append(bm.depsBackward[backupName], dep)
		}
	}
}

func (bm *backupMap) GetBadDeps() []string {
	var badBacks []string
	sort.Strings(bm.backupsExists)
	for _, k := range bm.backupsExists {
		if v, ok := bm.depsBackward[k]; ok {
			for _, b := range v {
				if !Contains(bm.backupsExists, b) || Contains(badBacks, b) {
					badBacks = append(badBacks, k)
					break
				}
			}
		}
	}
	return badBacks
}

func (bm *backupMap) GetFulls() []string {
	var fulls []string
	for _, b := range bm.backupsExists {
		if isFullBackup(b) {
			fulls = append(fulls, b)
		}
	}
	return fulls
}

func (bm *backupMap) GetFullsForDelete(maxFullBacks int) []string {
	fulls := bm.GetFulls()
	lenFulls := len(fulls)
	if lenFulls <= maxFullBacks {
		return []string{}
	}
	sort.Strings(fulls)
	return fulls[0 : lenFulls-maxFullBacks]
}

func (bm *backupMap) GetFullsForStore(maxFullBacks int) []string {
	fulls := bm.GetFulls()
	lenFulls := len(fulls)
	sort.Strings(fulls)
	if lenFulls <= maxFullBacks {
		return fulls
	}
	return fulls[lenFulls-maxFullBacks:]
}

func (bm *backupMap) GetBackupsForDelete(maxFullBacks int) []string {
	var backups []string
	for _, b := range bm.GetFullsForDelete(maxFullBacks) {
		backups = append(backups, b)
		backups = append(backups, bm.depsForward[b]...)
	}
	return backups
}

func (bm *backupMap) GetBackupsForStore(maxFullBacks int) []string {
	var backups []string
	for _, b := range bm.GetFullsForStore(maxFullBacks) {
		backups = append(backups, b)
		backups = append(backups, bm.depsForward[b]...)
	}
	return backups
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

func isFullBackup(name string) bool {
	if metaDirNameMatched(name, "F") {
		return true
	}
	return false
}

func metaDirNameMatched(metaDirName string, types ...string) bool {
	backupType := "FDIP"
	if len(types) > 0 {
		backupType = types[0]
	}
	matchString := fmt.Sprintf("^(\\d{8}_\\d{6}[%s]{1})$", backupType)
	if reMatch, _ := regexp.MatchString(matchString, metaDirName); reMatch {
		return true
	}
	return false
}
