package transport

import (
	"bufio"
	"cliback/config"
	"compress/gzip"
	"crypto/sha1"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"github.com/studio-b12/gowebdav"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"sort"
)

type TransportWebDav struct{}

func (twd *TransportWebDav) getConnectLink() string {
	c := config.New()
	link := ""
	if c.BackupStorage.BackupConn.Secure {
		link = "https://"
	} else {
		link = "http://"
	}
	link += fmt.Sprintf("%s:%d", c.BackupStorage.BackupConn.HostName, c.BackupStorage.BackupConn.Port)
	return link
}
func (twd *TransportWebDav) Do(file CliFile) (*TransportStat, error) {
	switch file.RunJobType {
	case Backup:
		return twd.Backup(file)
	case Restore:
		return twd.Restore(file)
	default:
		return nil, errTransCreate
	}
	return nil, errTransCreate
}

// MakeBackupTransportLocal archive file and returns meta info
func (twd *TransportWebDav) Backup(file CliFile) (*TransportStat, error) {
	c := config.New()
	t := new(TransportStat)
	Sha1Sum := sha1.New()
	wdCli := gowebdav.NewClient(twd.getConnectLink(), c.BackupStorage.BackupConn.UserName, c.BackupStorage.BackupConn.Password)
	if c.BackupStorage.BackupConn.Secure && c.BackupStorage.BackupConn.SkipVerify {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		wdCli.SetTransport(tr)
	}
	err := wdCli.Connect()
	if err != nil {
		return t, err
	}
	destFile := path.Join(c.BackupStorage.BackupDir, file.Archive())
	err = wdCli.MkdirAll(path.Dir(destFile), 0755)
	if err != nil {
		return t, err
	}
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
	err = wdCli.WriteStream(destFile, pr, 0644)
	if err != nil {
		return t, err
	}
	s, err := source.Stat()
	if err == nil {
		t.Size = s.Size()
	}
	d, err := wdCli.Stat(destFile)
	if err == nil {
		t.BSize = d.Size()
	}
	t.Sha1Sum = hex.EncodeToString(Sha1Sum.Sum(nil))
	return t, nil
}

// MakeRestoreTransportLocal restore file and returns meta info
func (twd *TransportWebDav) Restore(file CliFile) (*TransportStat, error) {
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

	wdCli := gowebdav.NewClient(twd.getConnectLink(), c.BackupStorage.BackupConn.UserName, c.BackupStorage.BackupConn.Password)
	if c.BackupStorage.BackupConn.Secure && c.BackupStorage.BackupConn.SkipVerify {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		wdCli.SetTransport(tr)
	}
	err = wdCli.Connect()
	if err != nil {
		return t, err
	}
	source, err := wdCli.ReadStream(path.Join(c.BackupStorage.BackupDir, file.Archive()))
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
	s, err := wdCli.Stat(path.Join(c.BackupStorage.BackupDir, file.Archive()))
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

func (twd *TransportWebDav) ReadMeta(mf *MetaFile) error {
	c := config.New()
	sha1sum := sha1.New()
	dest := bufio.NewWriter(&mf.Content)
	mwr := io.MultiWriter(sha1sum, dest)
	wdCli := gowebdav.NewClient(twd.getConnectLink(), c.BackupStorage.BackupConn.UserName, c.BackupStorage.BackupConn.Password)
	if c.BackupStorage.BackupConn.Secure && c.BackupStorage.BackupConn.SkipVerify {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		wdCli.SetTransport(tr)
	}
	err := wdCli.Connect()
	if err != nil {
		return err
	}
	var compressed bool
	_, err = wdCli.Stat(path.Join(c.BackupStorage.BackupDir, mf.Archive()))
	if err == nil {
		compressed = true
	} else {
		_, err = wdCli.Stat(path.Join(c.BackupStorage.BackupDir, mf.SPath()))
		if err != nil {
			if c.TaskArgs.Debug {
				log.Println(err)
			}
			return err
		}
	}
	var s io.Reader
	if compressed {
		source, err := wdCli.ReadStream(path.Join(c.BackupStorage.BackupDir, mf.Archive()))
		if err != nil {
			log.Println(err)
			return err
		}
		defer source.Close()
		bs, err := wdCli.Stat(path.Join(c.BackupStorage.BackupDir, mf.Archive()))
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
		source, err := wdCli.ReadStream(path.Join(c.BackupStorage.BackupDir, mf.SPath()))
		if err != nil {
			log.Println(err)
			return err
		}
		defer source.Close()
		bs, err := wdCli.Stat(path.Join(c.BackupStorage.BackupDir, mf.SPath()))
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

func (twd *TransportWebDav) WriteMeta(mf *MetaFile) error {
	c := config.New()
	sha1sum := sha1.New()
	wdCli := gowebdav.NewClient(twd.getConnectLink(), c.BackupStorage.BackupConn.UserName, c.BackupStorage.BackupConn.Password)
	if c.BackupStorage.BackupConn.Secure && c.BackupStorage.BackupConn.SkipVerify {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		wdCli.SetTransport(tr)
	}
	err := wdCli.Connect()
	if err != nil {
		return err
	}
	destFile := path.Join(c.BackupStorage.BackupDir, mf.Archive())
	err = wdCli.MkdirAll(path.Dir(destFile), 0775)
	if err != nil {
		return err
	}
	source := bufio.NewReader(&mf.Content)
	pr, pw := io.Pipe()
	gzw := gzip.NewWriter(pw)
	mwr := io.MultiWriter(gzw, sha1sum)
	go func() {
		defer pw.Close()
		defer gzw.Close()
		io.Copy(mwr, source)
		gzw.Flush()
	}()
	err = wdCli.WriteStream(destFile, pr, 0644)
	if err != nil {
		return err
	}

	_ = gzw.Flush()
	if err != nil {
		return err
	}
	mf.Sha1 = hex.EncodeToString(sha1sum.Sum(nil))
	s, err := wdCli.Stat(destFile)
	if err == nil {
		mf.BSize = s.Size()
	}
	return nil
}

func (twd *TransportWebDav) SearchMeta() ([]string, error) {
	var bnames []string
	c := config.New()
	wdCli := gowebdav.NewClient(twd.getConnectLink(), c.BackupStorage.BackupConn.UserName, c.BackupStorage.BackupConn.Password)
	if c.BackupStorage.BackupConn.Secure && c.BackupStorage.BackupConn.SkipVerify {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		wdCli.SetTransport(tr)
	}
	err := wdCli.Connect()
	if err != nil {
		return bnames, err
	}
	fileInfo, err := wdCli.ReadDir(c.BackupStorage.BackupDir)
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

func (twd *TransportWebDav) DeleteBackup(backupName string) error {
	c := config.New()
	wdCli := gowebdav.NewClient(twd.getConnectLink(), c.BackupStorage.BackupConn.UserName, c.BackupStorage.BackupConn.Password)
	if c.BackupStorage.BackupConn.Secure && c.BackupStorage.BackupConn.SkipVerify {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		wdCli.SetTransport(tr)
	}
	err := wdCli.Connect()
	if err != nil {
		return err
	}
	return wdCli.RemoveAll(path.Join(c.BackupStorage.BackupDir, backupName))
}
