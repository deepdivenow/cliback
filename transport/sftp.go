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

func MakeBackupTransportSFTP(file CliFile) (*transport, error) {
	c := config.New()
	t := new(transport)
	defer t.Cleanup()
	t.Sha1Sum = sha1.New()
	sp := sftp_pool.New()
	sftpCli, err := sp.GetClientLoop()
	if err != nil {
		return t, err
	}
	t.Closer = append(t.Closer, sp.MakeReleaseCloser(sftpCli))
	destFile := path.Join(c.BackupStorage.BackupDir, file.Archive())
	err = sftpCli.MkdirAll(path.Dir(destFile))
	if err != nil {
		return t, err
	}
	dest, err := sftpCli.Create(destFile)
	if err != nil {
		return t, err
	}
	t.Closer = append(t.Closer, dest)
	source, err := os.Open(file.BackupSrc())
	if err != nil {
		return t, err
	}
	t.Closer = append(t.Closer, source)
	gzw := gzip.NewWriter(dest)
	t.Closer = append(t.Closer, gzw)
	mwr := io.MultiWriter(gzw, t.Sha1Sum)
	t.Writer = mwr
	t.Reader = source
	t.Stater[0] = source
	t.Stater[1] = dest
	t.Flusher = append(t.Flusher, gzw)
	t.Ready = true
	return t, nil
}

func MakeRestoreTransportSFTP(file CliFile) (*transport, error) {
	c := config.New()
	t := new(transport)
	defer t.Cleanup()
	t.Sha1Sum = sha1.New()
	sp := sftp_pool.New()
	sftpCli, err := sp.GetClientLoop()
	if err != nil {
		return t, err
	}
	t.Closer = append(t.Closer, sp.MakeReleaseCloser(sftpCli))
	source, err := sftpCli.Open(path.Join(c.BackupStorage.BackupDir, file.Archive()))
	if err != nil {
		return t, err
	}
	t.Closer = append(t.Closer, source)
	destFile := path.Join(file.RestoreDest())
	err = MakeDirsRecurse(path.Dir(destFile))
	if err != nil {
		return t, err
	}
	dest, err := os.Create(destFile)
	if err != nil {
		return t, err
	}
	t.Closer = append(t.Closer, dest)
	gzr, err := gzip.NewReader(source)
	if err != nil {
		return t, err
	}
	t.Closer = append(t.Closer, gzr)
	mwr := io.MultiWriter(t.Sha1Sum, dest)
	t.Writer = mwr
	t.Reader = gzr
	t.Stater[1] = source
	t.Stater[0] = dest
	t.Ready = true
	return t, nil
}

func WriteMetaSFTP(mf *MetaFile) error {
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

func ReadMetaSFTP(mf *MetaFile) error {
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

func SearchMetaSFTP() ([]string, error) {
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
		if file.IsDir() {
			if reMatch, _ := regexp.MatchString("^(\\d{8}_\\d{6}[FDIP]{1})$", file.Name()); reMatch {
				bnames = append(bnames, file.Name())
			}
		}
	}
	sort.Strings(bnames)
	return bnames, nil
}
