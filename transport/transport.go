package transport

import (
	"cliback/config"
	"errors"
	"hash"
	"io"
)

type transport struct{
	Reader io.Reader
	Writer io.Writer
	Sha1Sum hash.Hash
	Closer []io.Closer
}

func (t *transport) Close() error{
	for _,h := range t.Closer{
		h.Close()
	}
	return nil
}

func (t *transport) Copy() (int64,error){
	return io.Copy(t.Writer,t.Reader)
}

func MakeTransport(file CliFile) (*transport,error){
	c := config.New()
	if (file.RunJobType == Backup){
		switch c.BackupStorage.Type {
		case "local":
			return MakeBackupTransportLocal(file)
		case "sftp":
			return MakeBackupTransportSFTP(file)
		default:
			return nil,errors.New("Transport not created")
		}
	}
	if (file.RunJobType == Restore){
		switch c.BackupStorage.Type {
		case "local":
			return MakeRestoreTransportLocal(file)
		case "sftp":
			return MakeRestoreTransportSFTP(file)
		default:
			return nil,errors.New("Transport not created")
		}
	}
	return nil,errors.New("Transport not created")
}

func ReadMeta(mf MetaFile) (MetaFile,error){
	c := config.New()
	switch c.BackupStorage.Type {
	case "local":
		return ReadMetaLocal(mf)
	case "sftp":
		return ReadMetaSFTP(mf)
	default:
		return mf,errors.New("Meta Read bad transport type")
	}
}

func WriteMeta(mf MetaFile) (error){
	c := config.New()
	switch c.BackupStorage.Type {
	case "local":
		return WriteMetaLocal(mf)
	case "sftp":
		return WriteMetaSFTP(mf)
	default:
		return errors.New("Meta Read bad transport type")
	}
}

func SearchMeta() ([]string,error){
	c := config.New()
	switch c.BackupStorage.Type {
	case "local":
		return SearchMetaLocal()
	case "sftp":
		return SearchMetaSFTP()
	default:
		return nil,errors.New("Meta Read bad transport type")
	}
}