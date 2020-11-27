package backupMap

import (
	"fmt"
	"testing"
)

func TestRetensionBackupMap(t *testing.T) {
	bm := New()
	mainBack := "20200107_000001I"
	depBacks := []string{"20200107_000001F"}
	bm.Add(mainBack, depBacks...)
	mainBack = "20200107_000002I"
	depBacks = []string{"20200107_000001F", "20200107_000001I"}
	bm.Add(mainBack, depBacks...)
	bm.Add("20200107_000001F")
	bm.Add("20200108_000001F")
	bm.Add("20200109_000001F")
	fmt.Println("Forward:", bm.depsForward)
	fmt.Println("Backward:", bm.depsBackward)
	fmt.Println("Exists:", bm.backupsExists)
	fmt.Println("Bad deps:", bm.GetBadDeps())
	fmt.Println("Fulls for Delete:", bm.GetFullsForDelete(2))
	fmt.Println("Backups for Delete:", bm.GetBackupsForDelete(2))
	fmt.Println("Finish")
}
