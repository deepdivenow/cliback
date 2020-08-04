package status

import "sync"

type FailType int

const (
	FailReadConfig        = 1
	FailInfo              = 1
	FailBackup            = 1
	FailBackupDatabase    = 2
	FailBackupTable       = 4
	FailBackupPartition   = 8
	FailBackupFile        = 16
	FailBackupMeta        = 32
	FailRestore           = 1
	FailRestoreDatabase   = 2
	FailRestoreTable      = 4
	FailRestorePartition  = 8
	FailRestoreFile       = 16
	FailRestoreMeta       = 32
	FailFreezeTable       = 64
	FailGetIncrement      = 64
	FailGetDBS            = 64
	FailGetTables         = 64
	FailClickhouseStorage = 64
)

type status struct {
	FinalStatus  int
	DetailStatus map[FailType]bool
}

var (
	once     sync.Once
	instance *status
)

func New() *status {
	once.Do(func() {
		instance = new(status)
		instance.DetailStatus = map[FailType]bool{}
	})
	return instance
}

func (s *status) SetStatus(failType FailType) {
	s.DetailStatus[failType] = true
}

func (s *status) GetFinalStatus() int {
	result := 0
	for k, v := range s.DetailStatus {
		if v {
			result = result | int(k)
		}
	}
	s.FinalStatus = result
	return result
}
