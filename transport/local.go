package transport

import (
	"bufio"
	"cliback/config"
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
)

type TransportLocal struct {
}

// MakeDirsRecurse make recursive dirs on local FS
func MakeDirsRecurse(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.MkdirAll(path, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}
func (tl *TransportLocal) Do(file CliFile) (*TransportStat, error) {
	switch file.RunJobType {
	case Backup:
		return tl.Backup(file)
	case Restore:
		return tl.Restore(file)
	default:
		return nil, errTransCreate
	}
	return nil, errTransCreate
}

// MakeBackupTransportLocal archive file and returns meta info
func (tl *TransportLocal) Backup(file CliFile) (*TransportStat, error) {
	c := config.New()
	t := new(TransportStat)
	Sha1Sum := sha1.New()
	destFile := path.Join(c.BackupStorage.BackupDir, file.Archive())
	err := MakeDirsRecurse(path.Dir(destFile))
	if err != nil {
		return t, err
	}
	dest, err := os.Create(destFile)
	if err != nil {
		return nil, err
	}
	defer dest.Close()
	source, err := os.Open(path.Join(file.BackupSrc()))
	if err != nil {
		return nil, err
	}
	defer source.Close()
	gzw := gzip.NewWriter(dest)
	defer gzw.Close()
	mwr := io.MultiWriter(gzw, Sha1Sum)
	_, err = io.Copy(mwr, source)
	if err != nil {
		return t, err
	}
	gzw.Flush()
	s, err := source.Stat()
	if err == nil {
		t.Size = s.Size()
	}
	d, err := dest.Stat()
	if err == nil {
		t.BSize = d.Size()
	}
	t.Sha1Sum = hex.EncodeToString(Sha1Sum.Sum(nil))
	return t, nil
}

// MakeRestoreTransportLocal restore file and returns meta info
func (tl *TransportLocal) Restore(file CliFile) (*TransportStat, error) {
	c := config.New()
	t := new(TransportStat)
	Sha1Sum := sha1.New()

	err := MakeDirsRecurse(path.Dir(file.RestoreDest()))
	if err != nil {
		return t, err
	}
	dest, err := os.Create(file.RestoreDest())
	if err != nil {
		return nil, err
	}
	defer dest.Close()

	source, err := os.Open(path.Join(c.BackupStorage.BackupDir, file.Archive()))
	if err != nil {
		return nil, err
	}
	defer source.Close()

	gzr, err := gzip.NewReader(source)
	if err != nil {
		return nil, err
	}
	defer gzr.Close()
	mwr := io.MultiWriter(Sha1Sum, dest)

	_, err = io.Copy(mwr, gzr)
	if err != nil {
		return t, err
	}
	s, err := source.Stat()
	if err == nil {
		t.BSize = s.Size()
	}
	d, err := dest.Stat()
	if err == nil {
		t.Size = d.Size()
	}
	t.Sha1Sum = hex.EncodeToString(Sha1Sum.Sum(nil))
	return t, nil
}

// WriteMetaLocal archive backup metafile and returns meta info
func (tl *TransportLocal) WriteMeta(mf *MetaFile) error {
	c := config.New()
	sha1sum := sha1.New()
	source := bufio.NewReader(&mf.Content)
	destFile := path.Join(c.BackupStorage.BackupDir, mf.Archive())
	err := MakeDirsRecurse(path.Dir(destFile))
	if err != nil {
		return err
	}
	dest, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer dest.Close()
	gzw := gzip.NewWriter(dest)
	defer gzw.Close()
	mwr := io.MultiWriter(gzw, sha1sum)
	mf.Size, err = io.Copy(mwr, source)
	if err != nil {
		return err
	}
	_ = gzw.Flush()
	mf.Sha1 = hex.EncodeToString(sha1sum.Sum(nil))
	s, err := dest.Stat()
	if err == nil {
		mf.BSize = s.Size()
	}
	return nil
}

// ReadMetaLocal restore backup metafile and returns meta info
func (tl *TransportLocal) ReadMeta(mf *MetaFile) error {
	c := config.New()
	sha1sum := sha1.New()
	dest := bufio.NewWriter(&mf.Content)
	mwr := io.MultiWriter(sha1sum, dest)
	var compressed bool
	_, err := os.Stat(path.Join(c.BackupStorage.BackupDir, mf.Archive()))
	if err == nil {
		compressed = true
	} else {
		_, err = os.Stat(path.Join(c.BackupStorage.BackupDir, mf.SPath()))
		if err != nil {
			if c.TaskArgs.Debug {
				log.Println(err)
			}
			return err
		}
	}
	var s io.Reader
	if compressed {
		source, err := os.Open(path.Join(c.BackupStorage.BackupDir, mf.Archive()))
		if err == nil {
		} else {
			return err
		}
		defer source.Close()
		gzw, err := gzip.NewReader(source)
		if err != nil {
			return err
		}
		defer gzw.Close()
		s = gzw
	} else {
		source, err := os.Open(path.Join(c.BackupStorage.BackupDir, mf.SPath()))
		if err != nil {
			log.Println(err)
			return err
		}
		defer source.Close()
		s = source
	}
	_, err = io.Copy(mwr, s)
	_ = dest.Flush()
	if err != nil {
		return err
	}
	mf.Sha1 = hex.EncodeToString(sha1sum.Sum(nil))
	return nil
}

// SearchMetaLocal search & returns backup names in archive
func (tl *TransportLocal) SearchMeta() ([]string, error) {
	var backupNames []string
	c := config.New()
	fileInfo, err := ioutil.ReadDir(c.BackupStorage.BackupDir)
	if err != nil {
		return backupNames, err
	}
	for _, file := range fileInfo {
		if file.IsDir() && metaDirNameMatched(file.Name()) {
			backupNames = append(backupNames, file.Name())
		}
	}
	sort.Strings(backupNames)
	return backupNames, nil
}

// DeleteBackupLocal delete backup from archive
func (tl *TransportLocal) DeleteBackup(backupName string) error {
	c := config.New()
	return os.RemoveAll(path.Join(c.BackupStorage.BackupDir, backupName))
}
