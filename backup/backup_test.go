package backup

import (
	"bytes"
	"cliback/config"
	"cliback/transport"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"testing"
)

func checkBackupType(bi *backupInfo) error {
	if len(bi.BackupFilter) < 1 {
		errors.New("BackupFilter not parsed")
	}
	if len(bi.Name) < 1 {
		errors.New("Name not parsed")
	}
	if len(bi.Type) < 1 {
		errors.New("Type not parsed")
	}
	if bi.BSize < 1 {
		errors.New("BSize not parsed")
	}
	if bi.Size < 1 {
		errors.New("Size not parsed")
	}
	if bi.RepoSize < 1 {
		errors.New("RepoSize not parsed")
	}
	if bi.RepoBSize < 1 {
		errors.New("RepoBSize not parsed")
	}
	return nil
}

func TestBackupRead(t *testing.T) {
	bi := new(backupInfo)
	jFile, err := ioutil.ReadFile("test_backup_v1.json")
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = json.Unmarshal(jFile, bi)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	err = checkBackupType(bi)
	if err != nil {
		t.Error(err.Error())
	}
	return
}

func TestMetaTransportLocalRead(t *testing.T) {
	c := config.New()
	c.BackupStorage.BackupDir = "/home/dro/go-1.13/src/cliback/backup"
	bi := new(backupInfo)
	mf := transport.MetaFile{
		Name:     "Test",
		Path:     "test_backup_v1.json",
		TryRetry: false,
		Sha1:     "",
		Content:  bytes.Buffer{},
	}
	err := transport.ReadMetaLocal(&mf)
	if err != nil {
		t.Error("Error read metafile")
	}
	err = json.Unmarshal(mf.Content.Bytes(), bi)
	if err != nil {
		t.Errorf("Unmarshal: %v", err)
	}
	err = checkBackupType(bi)
	if err != nil {
		t.Error(err.Error())
	}
	return
}
