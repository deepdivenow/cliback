package backup

import (
	"cliback/transport"
	"fmt"
	"sort"
)

func (bi *backupInfo) String() string {
	var outStr string
	outStr += fmt.Sprintf("%s backup: %s\n", bi.Type, bi.Name)
	outStr += fmt.Sprintf("\ttimestamp start/stop: %s / %s\n", bi.StartDate, bi.StopDate)
	outStr += fmt.Sprintf("\tdb size: %s backup size: %s\n", ByteCountIEC(bi.Size), ByteCountIEC(bi.BSize))
	outStr += fmt.Sprintf("\trepo size: %s repo backup size: %s\n", ByteCountIEC(bi.RepoSize), ByteCountIEC(bi.RepoBSize))
	if bi.Type == "diff" || bi.Type == "incr" {
		sort.Strings(bi.Reference)
		outStr += fmt.Sprintf("\treference: %s\n", bi.Reference)
	}
	if bi.Type == "part" {
		for db, dbInfo := range bi.DBS {
			for table, tableInfo := range dbInfo.Tables {
				outStr += fmt.Sprintf("\tdb: %s table: %s parts: %v\n", db, table, tableInfo.Partitions)
			}
		}
	}
	return outStr
}

func Info() error {
	tr, err := transport.MakeTransport()
	if err != nil {
		return err
	}
	metas, err := tr.SearchMeta()
	if err != nil {
		return err
	}
	for _, backupName := range metas {
		bi, err := BackupRead(backupName)
		if err == nil {
			fmt.Print(bi)
		}
	}
	return nil
}

func ByteCountSI(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}

func ByteCountIEC(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}
