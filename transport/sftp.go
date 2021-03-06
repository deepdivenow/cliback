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
	"sort"
)

type TransportSFTP struct {
}

func (ts *TransportSFTP) Do(file CliFile) (*TransportStat, error) {
	switch file.RunJobType {
	case Backup:
		return ts.Backup(file)
	case Restore:
		return ts.Restore(file)
	default:
		return nil, errTransCreate
	}
	return nil, errTransCreate
}

// MakeBackupTransportSFTP archive file and returns meta info
func (ts *TransportSFTP) Backup(file CliFile) (*TransportStat, error) {
	c := config.New()
	t := new(TransportStat)
	Sha1Sum := sha1.New()
	sp := sftp_pool.New()
	sftpCli, err := sp.GetClientLoop()
	if err != nil {
		return t, err
	}
	defer sp.ReleaseClient(sftpCli)
	destFile := path.Join(c.BackupStorage.BackupDir, file.Archive())
	err = sftpCli.MkdirAll(path.Dir(destFile))
	if err != nil {
		return t, err
	}
	dest, err := sftpCli.Create(destFile)
	if err != nil {
		return t, err
	}
	defer dest.Close()
	source, err := os.Open(file.BackupSrc())
	if err != nil {
		return t, err
	}
	defer source.Close()

	pr, pw := io.Pipe()
	gzw := gzip.NewWriter(pw)
	mwr := io.MultiWriter(gzw, Sha1Sum)
	go func() {
		defer pw.Close()
		defer gzw.Close()
		io.Copy(mwr, source)
		gzw.Flush()
	}()
	_, err = io.Copy(dest, pr)
	if err != nil {
		return t, err
	}
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

// MakeRestoreTransportSFTP restore file and returns meta info
func (ts *TransportSFTP) Restore(file CliFile) (*TransportStat, error) {
	c := config.New()
	t := new(TransportStat)
	Sha1Sum := sha1.New()

	destFile := path.Join(file.RestoreDest())
	err := MakeDirsRecurse(path.Dir(destFile))
	if err != nil {
		return t, err
	}
	dest, err := os.Create(destFile)
	if err != nil {
		return t, err
	}
	defer dest.Close()

	sp := sftp_pool.New()
	sftpCli, err := sp.GetClientLoop()
	if err != nil {
		return t, err
	}
	defer sp.ReleaseClient(sftpCli)
	source, err := sftpCli.Open(path.Join(c.BackupStorage.BackupDir, file.Archive()))
	if err != nil {
		return t, err
	}
	defer source.Close()

	gzr, err := gzip.NewReader(source)
	if err != nil {
		return t, err
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

// WriteMetaSFTP archive backup metafile and returns meta info
func (ts *TransportSFTP) WriteMeta(mf *MetaFile) error {
	c := config.New()
	sha1sum := sha1.New()
	sp := sftp_pool.New()
	sftpCli, err := sp.GetClientLoop()
	if err != nil {
		return err
	}
	defer sp.ReleaseClient(sftpCli)
	destFile := path.Join(c.BackupStorage.BackupDir, mf.Archive())
	err = sftpCli.MkdirAll(path.Dir(destFile))
	if err != nil {
		return err
	}
	dest, err := sftpCli.Create(destFile)
	if err != nil {
		return err
	}
	defer dest.Close()
	source := bufio.NewReader(&mf.Content)
	gzw := gzip.NewWriter(dest)
	defer gzw.Close()
	mwr := io.MultiWriter(gzw, sha1sum)
	mf.Size, err = io.Copy(mwr, source)
	_ = gzw.Flush()
	if err != nil {
		return err
	}
	mf.Sha1 = hex.EncodeToString(sha1sum.Sum(nil))
	s, err := dest.Stat()
	if err == nil {
		mf.BSize = s.Size()
	}
	return nil
}

// ReadMetaSFTP restore backup metafile and returns meta info
func (ts *TransportSFTP) ReadMeta(mf *MetaFile) error {
	c := config.New()
	sha1sum := sha1.New()
	dest := bufio.NewWriter(&mf.Content)
	mwr := io.MultiWriter(sha1sum, dest)
	sp := sftp_pool.New()
	sftpCli, err := sp.GetClientLoop()
	if err != nil {
		return err
	}
	defer sp.ReleaseClient(sftpCli)
	var compressed bool
	_, err = sftpCli.Stat(path.Join(c.BackupStorage.BackupDir, mf.Archive()))
	if err == nil {
		compressed = true
	} else {
		_, err = sftpCli.Stat(path.Join(c.BackupStorage.BackupDir, mf.SPath()))
		if err != nil {
			if c.TaskArgs.Debug {
				log.Println(err)
			}
			return err
		}
	}
	var s io.Reader
	if compressed {
		source, err := sftpCli.Open(path.Join(c.BackupStorage.BackupDir, mf.Archive()))
		if err != nil {
			log.Println(err)
			return err
		}
		defer source.Close()
		bs, err := source.Stat()
		if err == nil {
			mf.BSize = bs.Size()
		}
		gzr, err := gzip.NewReader(source)
		if err != nil {
			return err
		}
		defer gzr.Close()
		s = gzr
	} else {
		source, err := sftpCli.Open(path.Join(c.BackupStorage.BackupDir, mf.SPath()))
		if err != nil {
			log.Println(err)
			return err
		}
		defer source.Close()
		bs, err := source.Stat()
		if err == nil {
			mf.BSize = bs.Size()
		}
		s = source
	}

	mf.Size, err = io.Copy(mwr, s)
	_ = dest.Flush()
	if err != nil {
		return err
	}
	mf.Sha1 = hex.EncodeToString(sha1sum.Sum(nil))
	return nil
}

// SearchMetaSFTP search & returns backup names in archive
func (ts *TransportSFTP) SearchMeta() ([]string, error) {
	var bnames []string
	c := config.New()
	sp := sftp_pool.New()
	sftpCli, err := sp.GetClientLoop()
	if err != nil {
		return bnames, err
	}
	defer sp.ReleaseClient(sftpCli)
	fileInfo, err := sftpCli.ReadDir(c.BackupStorage.BackupDir)
	if err != nil {
		return bnames, err
	}
	for _, file := range fileInfo {
		if file.IsDir() && metaDirNameMatched(file.Name()) {
			bnames = append(bnames, file.Name())
		}
	}
	sort.Strings(bnames)
	return bnames, nil
}

// DeleteBackupSFTP delete backup from archive
func (ts *TransportSFTP) DeleteBackup(backupName string) error {
	c := config.New()
	sp := sftp_pool.New()
	sftpCli, err := sp.GetClientLoop()
	if err != nil {
		return err
	}
	defer sp.ReleaseClient(sftpCli)
	return sftp_pool.RemoveDirectoryRecursive(sftpCli, path.Join(c.BackupStorage.BackupDir, backupName))
}
