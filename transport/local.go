package transport

import (
	"bufio"
	"cliback/config"
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"sort"
)

func MakeDirsRecurse(path string){
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.MkdirAll(path, 0755)
	}
}
func MakeBackupTransportLocal(file CliFile) (*transport,error) {
	c := config.New()
	t := new(transport)
	defer t.Cleanup()
	t.Sha1Sum = sha1.New()
	dest_file := path.Join(c.BackupStorage.BackupDir,file.Archive())
	MakeDirsRecurse(path.Dir(dest_file))
	dest, err := os.Create(dest_file)
	if err != nil {
		return nil, err
	}
	t.Closer = append(t.Closer, dest)
	source,err := os.Open(path.Join(c.ShadowDir,file.Path))
	if err != nil {
		return nil, err
	}
	t.Closer = append(t.Closer, source)
	gzw := gzip.NewWriter(dest)
	t.Closer = append(t.Closer, gzw)
	mwr := io.MultiWriter(gzw,t.Sha1Sum)
	t.Writer = mwr
	t.Reader = source
	t.Ready=true
	return t,nil
}

func MakeRestoreTransportLocal(file CliFile) (*transport,error){
	c := config.New()
	t := new(transport)
	defer t.Cleanup()
	t.Sha1Sum = sha1.New()
	source,err := os.Open(path.Join(c.BackupStorage.BackupDir,file.Archive()))
	if err != nil {
		return nil, err
	}
	t.Closer = append(t.Closer, source)
	dest,err := os.Create(path.Join(c.ClickhouseDir,"data",file.Path))
	if err != nil {
		return nil,err
	}
	t.Closer = append(t.Closer, dest)
	gzw,err := gzip.NewReader(source)
	if err != nil {
		return nil,err
	}
	t.Closer = append(t.Closer, gzw)
	mwr := io.MultiWriter(t.Sha1Sum, dest)
	t.Writer = mwr
	t.Reader = gzw
	t.Ready=true
	return t,nil
}

func WriteMetaLocal(mf MetaFile) error{
	c := config.New()
	sha1sum := sha1.New()
	source := bufio.NewReader(&mf.Content)
	dest,err := os.Create(path.Join(c.BackupStorage.BackupDir,mf.Archive()))
	if err != nil {
		return err
	}
	defer dest.Close()
	gzw := gzip.NewWriter(dest)
	defer gzw.Close()
	mwr := io.MultiWriter(gzw,sha1sum)
	_,err=io.Copy(mwr,source)
	if err != nil {
		return err
	}
	mf.Sha1 = hex.EncodeToString(sha1sum.Sum(nil))
	return nil
}

func ReadMetaLocal(mf MetaFile) (MetaFile,error){
	c := config.New()
	sha1sum := sha1.New()
	dest:=bufio.NewWriter(&mf.Content)
	source,err := os.Open(path.Join(c.BackupStorage.BackupDir,mf.Archive()))
	if err != nil {
		return mf,err
	}
	defer source.Close()
	gzw,err := gzip.NewReader(source)
	if err != nil {
		return mf,err
	}
	defer gzw.Close()
	mwr := io.MultiWriter(sha1sum, dest)
	_,err=io.Copy(mwr,gzw)
	dest.Flush()
	if err != nil {
		return mf,err
	}
	mf.Sha1 = hex.EncodeToString(sha1sum.Sum(nil))
	return mf,nil
}

func SearchMetaLocal() ([]string,error){
	var bnames []string
	c := config.New()
	fileInfo, err := ioutil.ReadDir(c.BackupStorage.BackupDir)
	if err != nil {
		return bnames, err
	}
	for _, file := range fileInfo {
		if file.IsDir() {
			if re_match, _ := regexp.MatchString("^(\\d{8}_\\d{6}[FDIP]{1})$", file.Name()); re_match {
				bnames = append(bnames, file.Name())
			}
		}
	}
	sort.Strings(bnames)
	return bnames,nil
}