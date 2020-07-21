package transport

import (
	"bufio"
	"cliback/config"
	"cliback/sftp_pool"
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"sort"
)

func MakeBackupTransportSFTP(file CliFile) (*transport,error) {
	c := config.New()
	t := new(transport)
	t.Sha1Sum = sha1.New()
	sp:=sftp_pool.New()
	sftp_cli,err:=sp.GetClient()
	if err != nil{
		log.Println(err)
		return t,err
	}
	dest_file := path.Join(c.BackupStorage.BackupDir,file.Archive())
	sftp_cli.MkdirAll(path.Dir(dest_file))
	dest,err := sftp_cli.Create(dest_file)
	if err != nil {
		log.Println(err)
		return t,err
	}
	source,err := os.Open(path.Join(c.ShadowDir,file.Path))
	if err != nil {
		log.Println(err)
		return t,err
	}
	gzw := gzip.NewWriter(dest)
	mwr := io.MultiWriter(gzw,t.Sha1Sum)
	t.Closer = append(t.Closer, source)
	t.Closer = append(t.Closer, gzw)
	t.Closer = append(t.Closer, dest)
	t.Closer = append(t.Closer, sp.MakeReleaseCloser(sftp_cli))
	t.Writer = mwr
	t.Reader = source
	return t,nil
}

func MakeRestoreTransportSFTP(file CliFile) (*transport,error) {
	c := config.New()
	t := new(transport)
	t.Sha1Sum = sha1.New()

	sp:=sftp_pool.New()
	sftp_cli,err:=sp.GetClient()
	if err != nil{
		return t,err
	}
	source,err := sftp_cli.Open(path.Join(c.BackupStorage.BackupDir,file.Archive()))
	if err != nil {
		log.Println(err)
		return t,err
	}
	dest_file := path.Join(c.ClickhouseDir,"data",file.Path)
	MakeDirsRecurse(path.Dir(dest_file))
	dest,err := os.Create(dest_file)
	if err != nil {
		log.Println(err)
		return t, err
	}
	gzw,err := gzip.NewReader(source)
	if err != nil {
		return t,err
	}
	mwr := io.MultiWriter(t.Sha1Sum, dest)
	t.Closer = append(t.Closer, source)
	t.Closer = append(t.Closer, gzw)
	t.Closer = append(t.Closer, dest)
	t.Closer = append(t.Closer, sp.MakeReleaseCloser(sftp_cli))
	t.Writer = mwr
	t.Reader = gzw
	return t,nil
}

func WriteMetaSFTP(mf MetaFile) error{
	c := config.New()
	sha1sum := sha1.New()
	sp:=sftp_pool.New()
	sftp_cli,err:=sp.GetClient()
	if err != nil{
		return err
	}
	defer sp.ReleaseClient(sftp_cli)
	dest_file := path.Join(c.BackupStorage.BackupDir,mf.Archive())
	sftp_cli.MkdirAll(path.Dir(dest_file))
	dest,err := sftp_cli.Create(dest_file)
	if err != nil {
		return err
	}
	defer dest.Close()
	source := bufio.NewReader(&mf.Content)
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

func ReadMetaSFTP(mf MetaFile) (MetaFile,error){
	c := config.New()
	sha1sum := sha1.New()
	dest:=bufio.NewWriter(&mf.Content)

	sp:=sftp_pool.New()
	sftp_cli,err:=sp.GetClient()
	if err != nil{
		return mf,err
	}
	defer sp.ReleaseClient(sftp_cli)
	var compressed bool
	_,err=sftp_cli.Stat(path.Join(c.BackupStorage.BackupDir,mf.Archive()))
	if err == nil {
		compressed=true
	}else{
		_, err = sftp_cli.Stat(path.Join(c.BackupStorage.BackupDir, mf.Path))
		if err != nil {
			log.Println(err)
			return mf,err
		}
	}
	if (compressed) {
		source,err := sftp_cli.Open(path.Join(c.BackupStorage.BackupDir,mf.Archive()))
		if err != nil {
			log.Println(err)
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
		if err != nil {
			return mf,err
		}
		mf.Sha1 = hex.EncodeToString(sha1sum.Sum(nil))
		return mf,nil
	} else {
		source,err := sftp_cli.Open(path.Join(c.BackupStorage.BackupDir,mf.Path))
		if err != nil {
			log.Println(err)
			return mf,err
		}
		defer source.Close()
		mwr := io.MultiWriter(sha1sum, dest)
		_,err=io.Copy(mwr,source)
		if err != nil {
			return mf,err
		}
		mf.Sha1 = hex.EncodeToString(sha1sum.Sum(nil))
		return mf,nil
	}
}

func SearchMetaSFTP() ([]string,error){
	var bnames []string
	c := config.New()
	sp:=sftp_pool.New()
	sftp_cli,err:=sp.GetClient()
	if err != nil{
		return bnames,err
	}
	defer sp.ReleaseClient(sftp_cli)
	fileInfo, err := sftp_cli.ReadDir(c.BackupStorage.BackupDir)
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