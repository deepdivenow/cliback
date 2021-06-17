package transport

type TransportCommand struct{}

func (tc *TransportCommand) Do(file CliFile) (*TransportStat, error) {
	panic("implement me")
}

func (tc *TransportCommand) ReadMeta(mf *MetaFile) error {
	panic("implement me")
}

func (tc *TransportCommand) WriteMeta(mf *MetaFile) error {
	panic("implement me")
}

func (tc *TransportCommand) SearchMeta() ([]string, error) {
	panic("implement me")
}

func (tc *TransportCommand) DeleteBackup(backupName string) error {
	panic("implement me")
}
