package database

import (
	"cliback/config"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go"
)

var (
	once         sync.Once
	chdbInstance *ChDb
)

type ChMetaOpts struct {
	cutReplicated bool
}
type ChDb struct {
	dsn       string
	connect   *sql.DB
	reconnect bool
	mux       sync.Mutex
	metaOpts  ChMetaOpts
}

type TableInfo struct {
	DBName         string
	TableName      string
	DBNameFS       string
	TableNameFS    string
	DBPath         string
	TablePaths     []string
	DatabaseEngine string
	TableEngine    string
	DatabaseUUID   string
	TableUUID      string
}

func (ti *TableInfo) GetShortPath() string {
	switch ti.DatabaseEngine {
	case "Atomic":
		return path.Join("store", ti.TableUUID[0:3], ti.TableUUID)
	case "Ordinary":
		return path.Join("data", ti.GetDBNameE(), ti.GetTableNameE())
	default:
		return ""
	}
}
func (ti *TableInfo) GetDBNameE() string {
	return url.PathEscape(ti.DBName)
}
func (ti *TableInfo) GetTableNameE() string {
	return url.PathEscape(ti.TableName)
}

func New() *ChDb {
	once.Do(func() {
		chdbInstance = new(ChDb)
	})
	return chdbInstance
}

type dsnString struct {
	Host string
	Port string
	args map[string]interface{}
}

func CreateDsnString(host string, port uint16) dsnString {
	if len(host) < 1 {
		host = "localhost"
	}
	if port < 1 {
		port = 9000
	}
	return dsnString{
		Host: host,
		Port: strconv.FormatUint(uint64(port), 10),
		args: map[string]interface{}{},
	}
}

func (d dsnString) Add(argName string, argValue interface{}) {
	d.args[argName] = argValue
}

func (d dsnString) GetDSN() string {
	result := fmt.Sprintf("tcp://%s:%s", d.Host, d.Port)
	if len(d.args) < 1 {
		return result
	}
	delim := "?"
	for k, v := range d.args {
		switch v.(type) {
		case string:
			if len(v.(string)) > 0 {
				result += fmt.Sprintf("%s%s=%v", delim, k, v)
			} else {
				continue
			}
		case bool:
			if v.(bool) {
				result += fmt.Sprintf("%s%s=%v", delim, k, v)
			} else {
				continue
			}
		default:
			result += fmt.Sprintf("%s%s=%v", delim, k, v)
		}
		delim = "&"
	}
	return result
}

func (ch *ChDb) SetDSN(dsn config.Connection) {
	dsnStr := CreateDsnString(dsn.HostName, dsn.Port)
	dsnStr.Add("username", dsn.UserName)
	dsnStr.Add("password", dsn.Password)
	dsnStr.Add("secure", dsn.Secure)
	dsnStr.Add("skip_verify", dsn.SkipVerify)
	ch.dsn = dsnStr.GetDSN()
}
func (ch *ChDb) SetMetaOpts(cm config.ChMetaOpts) {
	ch.metaOpts.cutReplicated = cm.CutReplicated
}

func (ch *ChDb) Close() error {
	if ch.connect != nil {
		err := ch.connect.Close()
		if err == nil {
			ch.connect = nil
		}
		return err
	}
	return nil
}
func (ch *ChDb) ReConnect() error {
	if ch.connect != nil {
		if err := ch.connect.Ping(); err == nil {
			return nil
		} else {
			if exception, ok := err.(*clickhouse.Exception); ok {
				log.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
			} else {
				log.Println(err)
			}
			err = ch.Close()
			if err != nil {
				log.Printf("Database error: %s", err)
			}
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
	ch.connect = connect
	return nil
}

func (ch *ChDb) ReConnectLoop() error {
	for {
		err := ch.ReConnect()
		if err != nil {
			log.Printf("Error connect to Clickhouse: %s. Sleep 5s", err)
			time.Sleep(time.Second * 5)
			continue
		}
		return nil
	}
}

func (ch *ChDb) Execute(q string) (sql.Result, error) {
	ch.mux.Lock()
	defer ch.mux.Unlock()
	err := ch.ReConnectLoop()
	if err != nil {
		return nil, err
	}
	return ch.connect.Exec(q)
}
func (ch *ChDb) Query(q string) (*sql.Rows, error) {
	ch.mux.Lock()
	defer ch.mux.Unlock()
	err := ch.ReConnectLoop()
	if err != nil {
		return nil, err
	}
	return ch.connect.Query(q)
}
func (ch *ChDb) GetDBS() ([]string, error) {
	var result []string
	rows, err := ch.Query("SELECT DISTINCT database FROM system.parts WHERE active")
	if err != nil {
		return []string{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var dbName string
		err := rows.Scan(&dbName)
		if err == nil {
			result = append(result, dbName)
		}
	}
	if err := rows.Err(); err != nil {
		return []string{}, err
	}
	return result, nil
}
func (ch *ChDb) GetTables(db string) ([]string, error) {
	var result []string
	rows, err := ch.Query(fmt.Sprintf("SELECT DISTINCT table FROM system.parts WHERE active AND database = '%s'", db))
	if err != nil {
		return []string{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err == nil {
			result = append(result, table)
		}
	}
	if err := rows.Err(); err != nil {
		return []string{}, err
	}
	return result, nil
}
func (ch *ChDb) GetPartitions(db, table, part string) ([]string, error) {
	var result []string
	var query string
	if part == "" {
		query = fmt.Sprintf("SELECT DISTINCT partition FROM system.parts WHERE active AND database = '%s' AND table = '%s'", db, table)
	} else {
		query = fmt.Sprintf("SELECT DISTINCT partition FROM system.parts WHERE active AND database = '%s' AND table = '%s' AND partition LIKE '%s'", db, table, part)
	}
	rows, err := ch.Query(query)
	if err != nil {
		return []string{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var parttition string
		if err := rows.Scan(&parttition); err == nil {
			result = append(result, parttition)
		}
	}
	if err := rows.Err(); err != nil {
		return []string{}, err
	}
	return result, nil
}

func (ch *ChDb) QueryToMap(query string) (map[string]interface{}, error) {
	result := map[string]interface{}{}

	rows, err := ch.Query(query)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return result, err
	}
	colNum := len(cols)
	var values = make([]interface{}, colNum)
	for i, _ := range values {
		var ii interface{}
		values[i] = &ii
	}
	for rows.Next() {
		if err := rows.Scan(values...); err == nil {
		}
		//Scan only first line
		break
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	for i, colName := range cols {
		result[colName] = *(values[i].(*interface{}))
	}
	return result, nil
}

func (ch *ChDb) GetTableInfo(db, table string) (TableInfo, error) {
	result := TableInfo{
		DBName:    db,
		TableName: table,
	}
	query := fmt.Sprintf("SELECT * FROM system.databases WHERE name = '%s'", db)
	dbRes, err := ch.QueryToMap(query)
	if err != nil {
		return result, err
	}
	result.DatabaseUUID = GetStringFromMapInterface(dbRes, "uuid")
	result.DatabaseEngine = GetStringFromMapInterface(dbRes, "engine")
	result.DBPath = GetStringFromMapInterface(dbRes, "data_path")
	query = fmt.Sprintf("SELECT * FROM system.tables WHERE database = '%s' AND name = '%s'", db, table)
	tbRes, err := ch.QueryToMap(query)
	if err != nil {
		return result, err
	}
	result.TableUUID = GetStringFromMapInterface(tbRes, "uuid")
	result.TableEngine = GetStringFromMapInterface(tbRes, "engine")
	result.TablePaths = GetStringsFromMapInterface(tbRes, "data_paths")
	return result, nil
}

func (ch *ChDb) GetDisks() (map[string]string, error) {
	result := map[string]string{}
	query := "SELECT name,path FROM system.disks"
	rows, err := ch.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var disk, dbPath string
		if err := rows.Scan(&disk, &dbPath); err == nil {
			result[disk] = dbPath
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
func (ch *ChDb) GetDBProps(dbName string) (map[string]string, error) {
	result := map[string]string{}
	query := fmt.Sprintf("SELECT name,engine,data_path,metadata_path,uuid FROM system.databases WHERE name='%s'", dbName)
	rows, err := ch.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		scan := []interface{}{}
		for i := 0; i < len(cols); i++ {
			str := ""
			scan = append(scan, &str)
		}
		if err := rows.Scan(scan...); err == nil {
			for i, c := range cols {
				result[c] = *scan[i].(*string)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
func (ch *ChDb) FreezeTable(db, table, part string) error {
	var query string
	if part == "" {
		query = fmt.Sprintf("ALTER TABLE `%s`.`%s` FREEZE", db, table)
	} else if reMatch, _ := regexp.MatchString("^(\\d+)$", part); reMatch {
		query = fmt.Sprintf("ALTER TABLE `%s`.`%s` FREEZE PARTITION %s", db, table, part)
	} else {
		query = fmt.Sprintf("ALTER TABLE `%s`.`%s` FREEZE PARTITION '%s'", db, table, part)
	}
	_, err := ch.Execute(query)
	return err
}
func (ch *ChDb) GetIncrement() (int, error) {
	c := config.New()
	b, err := ioutil.ReadFile(path.Join(c.ClickhouseStorage["default"], "shadow/increment.txt"))
	if err != nil {
		return 0, err
	}

	lines := strings.Split(string(b), "\n")
	// Assign cap to avoid resize on every append.
	for _, l := range lines {
		// Empty line occurs at the end of the file when we use Split.
		if len(l) == 0 {
			continue
		}
		// Atoi better suits the job when we know exactly what we're dealing
		// with. Scanf is the more general option.
		n, err := strconv.Atoi(l)
		if err != nil {
			return 0, err
		}
		return n, nil
	}
	return 0, nil
}
func (ch *ChDb) CreateDatabase(db string) error {
	var query string
	query = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", db)
	_, err := ch.Execute(query)
	return err
}
func ReplaceAttachToCreateTable(db, table, meta string) string {
	meta = strings.Replace(meta, "CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1)
	meta = strings.Replace(meta, fmt.Sprintf("ATTACH TABLE %s", table), fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s`", db, table), 1)
	meta = strings.Replace(meta, fmt.Sprintf("ATTACH TABLE `%s`", table), fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s`", db, table), 1)
	return meta
}
func ReplaceCutReplicatedTable(meta string) string {
	re := regexp.MustCompile("ENGINE\\s=\\sReplicated\\w*MergeTree")
	if !re.MatchString(meta) {
		return meta
	}
	engineLabelPos := re.FindStringIndex(meta)
	endStrings := []string{"PARTITION BY", "ORDER BY", "SETTINGS", "PRIMARY KEY", "SAMPLE BY", "TTL"}
	endPos := -1
	for _, es := range endStrings {
		ep := strings.Index(meta, es)
		if ep >= 0 {
			if endPos == -1 || ep < endPos {
				endPos = ep
			}
		}
	}
	if endPos < 0 {
		args := strings.Split(meta[engineLabelPos[1]:], ", ")
		argsStr := "(" + strings.Join(args[2:], ",")
		return meta[:engineLabelPos[0]] + "ENGINE = MergeTree" + argsStr
	}
	return meta[:engineLabelPos[0]] + "ENGINE = MergeTree()\n " + meta[endPos:]
}
func (ch *ChDb) CreateTable(db, table, meta string) error {
	meta = ReplaceAttachToCreateTable(db, table, meta)
	if ch.metaOpts.cutReplicated {
		meta = ReplaceCutReplicatedTable(meta)
	}
	log.Printf("Create Table:\n%s", meta)
	_, err := ch.Execute(meta)
	return err
}
func (ch *ChDb) ShowCreateTable(db, table string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", db, table)
	log.Printf("Get Table Meta: `%s`.`%s`", db, table)
	var result []string
	rows, err := ch.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var partition string
		if err := rows.Scan(&partition); err == nil {
			result = append(result, partition)
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(result) == 0 {
		return "", errors.New("Execute show create table return nil")
	}
	return result[0], nil
}
func isInteregerPart(part string) bool {
	reMatch, _ := regexp.MatchString("^\\d+$", part)
	return reMatch
}

func (ch *ChDb) AttachPartition(db, table, part string) error {
	var query, logFormat string
	if isInteregerPart(part) {
		query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PARTITION %s", db, table, part)
		logFormat = "Attach integer part `%s`.`%s`.%s"
	} else {
		query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PARTITION '%s'", db, table, part)
		logFormat = "Attach string part `%s`.`%s`.'%s'"
	}
	log.Printf(logFormat, db, table, part)
	_, err := ch.Execute(query)
	return err
}
func (ch *ChDb) AttachPartitionByDir(db, table, dir string) error {
	query := fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PARTITION ID '%s'", db, table, dir)
	log.Printf("Attach Unknown part AS dir `%s`.`%s`.%s", db, table, dir)
	_, err := ch.Execute(query)
	return err
}

// Contains tells whether a contains x.
func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func GetStringFromMapInterface(m map[string]interface{}, k string) string {
	var v interface{}
	var ok bool
	if v, ok = m[k]; !ok {
		return ""
	}
	switch v.(type) {
	case string:
		return v.(string)
	default:
		return ""
	}
	return ""
}

func GetStringsFromMapInterface(m map[string]interface{}, k string) []string {
	var v interface{}
	var ok bool
	if v, ok = m[k]; !ok {
		return []string{}
	}
	switch v.(type) {
	case []string:
		return v.([]string)
	default:
		return []string{}
	}
	return []string{}
}
