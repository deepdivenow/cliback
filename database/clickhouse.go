package database

import (
	"cliback/config"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"log"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	once          sync.Once
	chdb_instance *ChDb
)

type ChMetaOpts struct {
	cut_replicated bool
}

type ChDb struct {
	dsn string
	connect *sql.DB
	reconnect bool
	mux sync.Mutex
	metaopts ChMetaOpts
}

func New() *ChDb {
	once.Do(func() {
		chdb_instance = new(ChDb)
	})
	return chdb_instance
}

func (ch *ChDb) SetDSN(dsn config.Connection) {
	var host,port string
	if len(dsn.HostName) > 0{
		host = dsn.HostName
	} else {
		host = "localhost"
	}
	if dsn.Port < 1{
		port="9000"
	} else {
		port=strconv.FormatUint(uint64(dsn.Port),10)
	}
	if len(dsn.UserName) > 0{
		if len(dsn.Password) > 0{
			ch.dsn=fmt.Sprintf("tcp://%s:%s?username=%s&password=%s",host,port,dsn.UserName,dsn.Password)
		} else {
			ch.dsn=fmt.Sprintf("tcp://%s:%s?username=%s",host,port,dsn.UserName)
		}

		} else {
		ch.dsn=fmt.Sprintf("tcp://%s:%s",host,port)
	}
}

func (ch *ChDb) Close() error {
	if ch.connect != nil{
		err:=ch.connect.Close()
		if err == nil{
			ch.connect=nil
		}
		return err
	}
	return nil
}

func (ch *ChDb) ReConnect() error {
	if ch.connect != nil{
		if err := ch.connect.Ping(); err == nil {
			return nil
		} else {
			if exception, ok := err.(*clickhouse.Exception); ok {
				log.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
			} else {
				log.Println(err)
			}
			ch.Close()
		}
	}
	connect, err := sql.Open("clickhouse", ch.dsn)
	if err != nil {
		return err
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			log.Println(err)
		}
		return err
	}
	ch.connect=connect
	return nil
}
func (ch *ChDb) ReConnectLoop() error{
	for {
		err := ch.ReConnect()
		if err!=nil{
			log.Printf("Error connect to Clickhouse: %s. Sleep 5s",err)
			time.Sleep(time.Second*5)
			continue
		}
		return nil
	}
	return nil
}

func (ch *ChDb) Execute(q string) (sql.Result, error) {
	ch.mux.Lock()
	defer ch.mux.Unlock()
	err:=ch.ReConnectLoop()
	if err!=nil{
		return nil, err
	}
	return ch.connect.Exec(q)
}
func (ch *ChDb) Query(q string) (*sql.Rows, error) {
	ch.mux.Lock()
	defer ch.mux.Unlock()
	err:=ch.ReConnectLoop()
	if err!=nil{
		return nil, err
	}
	return ch.connect.Query(q)
}
func (ch *ChDb) GetDBS() ([]string, error) {
	var result []string
	rows,err:=ch.Query("SELECT DISTINCT database FROM system.parts WHERE active")
	if err != nil {
		return []string{},err
	}
	defer rows.Close()
	for rows.Next() {
		var db_name string
		err:=rows.Scan(&db_name)
		if  err == nil{
			result=append(result, db_name)
		}
	}
	if err := rows.Err(); err != nil {
		return []string{},err
	}
	return result,nil
}
func (ch *ChDb) GetTables(db string) ([]string, error) {
	var result []string
	rows,err:=ch.Query(fmt.Sprintf("SELECT DISTINCT table FROM system.parts WHERE active AND database = '%s'",db))
	if err != nil {
		return []string{},err
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		if err:=rows.Scan(&table); err == nil{
			result=append(result, table)
		}
	}
	if err := rows.Err(); err != nil {
		return []string{},err
	}
	return result,nil
}
func (ch *ChDb) GetPartitions(db,table,part string) ([]string, error) {
	var result []string
	var query string
	if part=="" {
		query=fmt.Sprintf("SELECT DISTINCT partition FROM system.parts WHERE active AND database = '%s' AND table = '%s'",db,table)
	} else {
		query=fmt.Sprintf("SELECT DISTINCT partition FROM system.parts WHERE active AND database = '%s' AND table = '%s' AND partition LIKE '%s'",db,table,part)
	}
	rows,err:=ch.Query(query)
	if err != nil {
		return []string{},err
	}
	defer rows.Close()
	for rows.Next() {
		var parttition string
		if err:=rows.Scan(&parttition); err == nil{
			result=append(result, parttition)
		}
	}
	if err := rows.Err(); err != nil {
		return []string{},err
	}
	return result,nil
}
func (ch *ChDb) GetFNames(db,table,part string) (string, error) {
	var result []string
	var query string
	if part=="" {
		query=fmt.Sprintf("SELECT DISTINCT path FROM system.parts WHERE active AND database = '%s' AND table = '%s' LIMIT 1",db,table)
	} else {
		query=fmt.Sprintf("SELECT DISTINCT path FROM system.parts WHERE active AND database = '%s' AND table = '%s' AND partition LIKE '%s' LIMIT 1",db,table,part)
	}
	rows,err:=ch.Query(query)
	if err != nil {
		return "",err
	}
	defer rows.Close()
	for rows.Next() {
		var path string
		if err:=rows.Scan(&path); err == nil{
			result=append(result, path)
		}
	}
	if err := rows.Err(); err != nil {
		return "",err
	}
	f_path:=result[0]
	if f_path[len(f_path)-1] == '/'{
		f_path=f_path[:len(f_path)-1]
	}
	_,l_dir:=path.Split(f_path)
	return l_dir,nil
}
func (ch *ChDb) FreezeTable(db,table,part string) (error) {
	var query string
	if part=="" {
		query=fmt.Sprintf("ALTER TABLE `%s`.`%s` FREEZE",db,table)
	} else if re_match, _ := regexp.MatchString("^(\\d+)$", part); re_match{
		query=fmt.Sprintf("ALTER TABLE `%s`.`%s` FREEZE PARTITION %s",db,table,part)
	} else {
		query=fmt.Sprintf("ALTER TABLE `%s`.`%s` FREEZE PARTITION '%s'",db,table,part)
	}
	_,err:=ch.Execute(query)
	return err
}
func (ch *ChDb) CreateDatabase(db string) (error) {
	var query string
	query=fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`",db)
	_,err:=ch.Execute(query)
	return err
}
func ReplaceAttachToCreateTable(db,table,meta string) (string){
	meta=strings.Replace(meta, fmt.Sprintf("ATTACH TABLE %s",table),fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s`",db,table),1)
	meta=strings.Replace(meta, fmt.Sprintf("ATTACH TABLE `%s`",table),fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s`",db,table),1)
	return meta
}
func ReplaceCutReplicatedTable(meta string) (string){
	re := regexp.MustCompile("ENGINE\\s=\\sReplicated\\w*MergeTree")
	if  !re.MatchString(meta){ return meta }
	engine_label_pos:=re.FindStringIndex(meta)
	end_strings:= []string{"PARTITION BY", "ORDER BY", "SETTINGS", "PRIMARY KEY", "SAMPLE BY", "TTL"}
	end_pos := -1
	for _,es := range(end_strings){
		ep:= strings.Index(meta, es)
		if ep >=0{
			if end_pos == -1 || ep<end_pos{
				end_pos=ep
			}
		}
	}
	if end_pos < 0{
		args:=strings.Split(meta[engine_label_pos[1]:],", ")
		args_str:= "(" + strings.Join(args[2:],",")
		return meta[:engine_label_pos[0]]+"ENGINE = MergeTree"+args_str
	}
	return meta[:engine_label_pos[0]]+"ENGINE = MergeTree"+meta[engine_label_pos[1]:]
}
func (ch *ChDb) CreateTable(db,table,meta string) (error) {
	meta=ReplaceAttachToCreateTable(db,table,meta)
	if ch.metaopts.cut_replicated {
		meta = ReplaceCutReplicatedTable(meta)
	}
	log.Printf("Create Table:\n%s",meta)
	_,err:=ch.Execute(meta)
	return err
}
func (ch *ChDb) AttachPartition(db,table,part string) (error){
	var query,log_format string
	if re_match, _ := regexp.MatchString("\\d+", part); re_match{
		query=fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PARTITION %s",db,table,part)
		log_format="Attach integer part `%s`.`%s`.%s"
	} else {
		query=fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PARTITION '%s'",db,table,part)
		log_format="Attach string part `%s`.`%s`.'%s'"
	}
	log.Printf(log_format,db,table,part)
	_,err:=ch.Execute(query)
	return err
}
func (ch *ChDb) AttachPartitionByDir(db,table,dir string) (error){
	query:=fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PARTITION ID '%s'",db,table,dir)
	_,err:=ch.Execute(query)
	return err
}