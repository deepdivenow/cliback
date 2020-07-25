package backup

import (
	"cliback/transport"
	"fmt"
)

func(bi *backup_info) String() string{
	var outStr string
	outStr+=fmt.Sprintf("%s backup: %s\n", bi.Type,bi.Name)
	outStr+=fmt.Sprintf("\ttimestamp start/stop: %s / %s\n", bi.StartDate,bi.StopDate)
	outStr+=fmt.Sprintf("\tdb size: %d backup size: %d\n", bi.Size, bi.RepoSize)
	outStr+=fmt.Sprintf("\trepo size: %d repo backup size: %d\n", bi.BSize, bi.RepoBSize)
	if bi.Type == "diff" || bi.Type == "incr"{
		outStr+=fmt.Sprintf("\treference: %s\n", bi.Reference)
	}
	if bi.Type == "part"{
		for db,db_info := range(bi.DBS){
			for table,table_info := range(db_info.Tables){
				outStr+=fmt.Sprintf("\tdb: %s table: %s parts: %v\n", db, table, table_info.Partitions)
			}
		}
	}
	return outStr
}

func Info() error{
	metas,err := transport.SearchMeta()
	if err != nil{
		return err
	}
	for _,back_name := range(metas){
		bi,err := BackupRead(back_name)
		if err == nil {
			fmt.Print(bi)
		}
	}
	return nil
}