package main

import (
	"cliback/config"
	"cliback/transport"
	"cliback/workerpool"
	"encoding/hex"
	"log"
	"os"
	"path/filepath"
)

func FindFiles(dir_for_backup string, jobs_chan chan<- workerpool.TaskElem){
	err := filepath.Walk(dir_for_backup,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if (info.IsDir()){
				return nil
			}
			cPath := path[len(dir_for_backup):]
			cliF:=transport.CliFile{
				Name:      cPath,
				Path:      cPath,
				RunJobType: transport.Backup,
				TryRetry:   false,
			}
			jobs_chan <- cliF
			return nil
		})
	if err != nil {
		log.Println(err)
	}
	
	//for _, f := range files {
	//	//fmt.Println(f.Name())
	//	cliF:=transport.CliFile{
	//		Name:      f.Name(),
	//		Path:      f.Name(),
	//		RunJobType: transport.Backup,
	//		TryRetry:   false,
	//	}
	//
	//cliF := transport.CliFile{
	//	Name:       "Hello",
	//	Path:       "/tmp/some_file",
	//	RunJobType: transport.Backup,
	//	TryRetry:   false,
	//}
	//tr,_ := transport.MakeTransport(cliF)
	//count,err:=tr.Copy()
	//println(count)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//tr.Close()
	//fmt.Printf("% x", tr.Sha1Sum.Sum(nil))
	//fmt.Println()
	//cliF2 := transport.CliFile{
	//	Name:       "Hello",
	//	Path:       "/one/hello",
	//	RunJobType: transport.Restore,
	//	TryRetry:   false,
	//}
	//tr2,_ := transport.MakeTransport(cliF2)
	//tr2.Copy()
	//fmt.Printf("% x", tr2.Sha1Sum.Sum(nil))
	//fmt.Println()
	//tr2.Close()
	//	jobs_chan <- cliF
	//}
	close(jobs_chan)
}
func Run(cf transport.CliFile) (transport.CliFile, error){
	tr,_:=transport.MakeTransport(cf)
	_,err:=tr.Copy()
	//println(count)
	if err != nil {
		log.Fatal(err)
	}
	tr.Close()
	cf.Sha1 = hex.EncodeToString(tr.Sha1Sum.Sum(nil))
	return cf,nil
}

func main(){
	c := config.New()
	c.Read("/home/dro/go-1.13/src/cliback/clickhouse_backup.yaml")
	c.SetShadow("/tmp/dir_for_backup")

	var wp_task workerpool.TaskFunc = func(i interface{}) (interface{}, error) {
		field, _ := i.(transport.CliFile)
		return Run(field)
	}

	wp := workerpool.MakeWorkerPool(wp_task,4,3,10)
	wp.Start()
	go FindFiles(c.ShadowDir,wp.Get_Jobs_Chan())
	for job := range(wp.Get_Results_Chan()){
		j,_ := job.(transport.CliFile)
		println(j.Name,j.Sha1)
	}

	//
	//cliF := transport.CliFile{
	//	Name:       "Hello",
	//	Path:       "/tmp/some_file",
	//	RunJobType: transport.Backup,
	//	TryRetry:   false,
	//}
	//tr,_ := transport.MakeTransport(cliF)
	//count,err:=tr.Copy()
	//println(count)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//tr.Close()
	//fmt.Printf("% x", tr.Sha1Sum.Sum(nil))
	//fmt.Println()
	//cliF2 := transport.CliFile{
	//	Name:       "Hello",
	//	Path:       "/one/hello",
	//	RunJobType: transport.Restore,
	//	TryRetry:   false,
	//}
	//tr2,_ := transport.MakeTransport(cliF2)
	//tr2.Copy()
	//fmt.Printf("% x", tr2.Sha1Sum.Sum(nil))
	//fmt.Println()
	//tr2.Close()
}
