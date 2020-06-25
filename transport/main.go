package transport

import (
	"cliback/config"
	"compress/gzip"
	"crypto/sha1"
	"hash"
	"io"
	"log"
	"os"
	"path"
)

type transport struct{
	Reader io.Reader
	Writer io.Writer
	Sha1Sum hash.Hash
	Closer []io.Closer
}

func (t *transport) Close() (error){
	for _,h := range t.Closer{
		h.Close()
	}
	return nil
}

func (t *transport) Copy() (int64,error){
	return io.Copy(t.Writer,t.Reader)
}

func MakeDirsRecurse(path string){
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.MkdirAll(path, 0755)
	}
}

func MakeTransport(file CliFile) (*transport,error){
	if (file.RunJobType == Backup){
		return MakeBackupTransport(file)
	}
	if (file.RunJobType == Restore){
		return MakeRestoreTransport(file)
	}
	return nil,nil
}

func MakeBackupTransport(file CliFile) (*transport, error) {
	c := config.New()
	t := new(transport)
	t.Sha1Sum = sha1.New()
	var dest,source *os.File
	switch c.BackupStorage.Type {
		case "local":
			var err error
			dest_file := path.Join(c.BackupStorage.BackupDir,file.Archive())
			MakeDirsRecurse(path.Dir(dest_file))
			dest, err = os.Create(dest_file)
			if err != nil {
				log.Fatal(err)
			}
			source,err = os.Open(path.Join(c.ShadowDir,file.Path))
			if err != nil {
				log.Fatal(err)
			}
	default:
		log.Fatal("Bad transport type")
		os.Exit(1)
	}
	gzw := gzip.NewWriter(dest)
	mwr := io.MultiWriter(gzw,t.Sha1Sum)
	t.Closer = append(t.Closer, source)
	t.Closer = append(t.Closer, gzw)
	t.Closer = append(t.Closer, dest)
	t.Writer = mwr
	t.Reader = source
	return t,nil
}

func MakeRestoreTransport(file CliFile) (*transport,error){
	t := new(transport)
	t.Sha1Sum = sha1.New()
	source,err := os.Open("/tmp/some_file.gz")
	if err != nil {
		log.Fatal(err)
	}
	dest,err := os.Create("/tmp/some_file2")
	if err != nil {
		log.Fatal(err)
	}
	gzw,err := gzip.NewReader(source)
	if err != nil {
		log.Fatal(err)
	}
	mwr := io.MultiWriter(t.Sha1Sum, dest)
	t.Closer = append(t.Closer, source)
	t.Closer = append(t.Closer, gzw)
	t.Closer = append(t.Closer, dest)
	t.Writer = mwr
	t.Reader = gzw
	return t,nil
}
