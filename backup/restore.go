package backup

import (
	"bytes"
	"cliback/transport"
	"encoding/json"
	"errors"
	"log"
	"path"
)

func Restore() error{
	// Main restore loop
	//var restore_objects map[string][]string
	bi1,err := Backupv1Read()
	if err != nil{
		return err
	}
	switch bi1.Version{
	case 1:
		return Restorev1(bi1)
	case 2:
		bi2,err := Backupv2Read()
		if err != nil{
			return err
		}
		return Restorev2(bi2)
	default:
		return errors.New("Error read backup info version")
	}
}

func Restorev1(bi1 *backup_info_v1) error{
	return nil
}
func Restorev2(bi1 *backup_info_v2) error{
	return nil
}

func get_restore_objects() (map[string][]string,error) {
	var restore_objects map[string][]string


	return restore_objects, nil
}

func restore_table(db,table,part string) (error) {
	return nil
}

func Backupv1Read(backup_name string) (*backup_info_v1,error) {
	bi := new(backup_info_v1)
	mf := transport.MetaFile{
		Name:     "Main backup JSON",
		Path:     path.Join(backup_name,"backup.json"),
		TryRetry: false,
		Sha1:     "",
		Content:  bytes.Buffer{},
	}
	mf,err:=transport.ReadMeta(mf)
	if err != nil{
		log.Println("Error read metafile")
		return nil, err
	}
	err = json.Unmarshal(mf.Content.Bytes(), bi)
	if err != nil {
		log.Println("Unmarshal: %v", err)
		return nil, err
	}
	return bi,nil
}
func Backupv2Read(backup_name string) (*backup_info_v2,error) {
	bi := new(backup_info_v2)
	mf := transport.MetaFile{
		Name:     "Main backup JSON",
		Path:     path.Join(backup_name,"backup.json"),
		TryRetry: false,
		Sha1:     "",
		Content:  bytes.Buffer{},
	}
	mf,err:=transport.ReadMeta(mf)
	if err != nil{
		log.Println("Error read metafile")
		return nil, err
	}
	err = json.Unmarshal(mf.Content.Bytes(), bi)
	if err != nil {
		log.Println("Unmarshal: %v", err)
		return nil, err
	}
	return bi,nil
}