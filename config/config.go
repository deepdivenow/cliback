package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"strconv"
	"sync"

	"gopkg.in/yaml.v2"
)

type Connection struct {
	HostName    string `yaml:"hostname"`
	UserName    string `yaml:"username,omitempty"`
	Password    string `yaml:"password,omitempty"`
	Port        uint16 `yaml:"port,omitempty"`
	KeyFilename string `yaml:"key_filename,omitempty"`
	Secure      bool   `yaml:"secure,omitempty"`
	SkipVerify  bool   `yaml:"skip_verify,omitempty"`
}

type backupStorage struct {
	Type       string     `yaml:"type"`
	BackupDir  string     `yaml:"backup_dir"`
	BackupConn Connection `yaml:"backup_conn"`
}

type RunJobType int

const (
	Backup RunJobType = iota + 1
	Restore
	Info
)

type taskArgs struct {
	JobType      RunJobType
	JobName      string
	JobPartition string
	BackupType   string
	Debug        bool
	DBNow        string
	TableNow     string
}

type ChMetaOpts struct {
	CutReplicated          bool `yaml:"replace_replicated_to_default"`
	BadStorageToDefault    bool `yaml:"move_bad_storage_to_default"`
	FailIfStorageNotExists bool `yaml:"fail_if_storage_not_exists"`
}

type WorkerPoolT struct {
	NumWorkers int `yaml:"num_workers"`
	NumRetry   int `yaml:"num_retry"`
	ChanLen    int `yaml:"chan_len"`
}

type config struct {
	BackupStorage         backupStorage       `yaml:"backup_storage"`
	ShadowDirIncr         int                 `yaml:"-"`
	TaskArgs              taskArgs            `yaml:"-"`
	ClickhouseBackupConn  Connection          `yaml:"clickhouse_backup_conn"`
	ClickhouseRestoreConn Connection          `yaml:"clickhouse_restore_conn"`
	ClickhouseRestoreOpts ChMetaOpts          `yaml:"clickhouse_restore_opts"`
	ClickhouseStorage     map[string]string   `yaml:"clickhouse_storage"`
	BackupFilter          map[string][]string `yaml:"backup_filter"`
	RestoreFilter         map[string][]string `yaml:"restore_filter"`
	WorkerPool            WorkerPoolT         `yaml:"worker_pool"`
	RetentionBackupFull   int                 `yaml:"retention_backup_full"`
}

var (
	once     sync.Once
	instance *config
)

func New() *config {
	once.Do(func() {
		instance = new(config)
	})
	return instance
}

func (c *config) Read(filename string) error {
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
		return err
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Printf("Unmarshal: %v", err)
		return err
	}
	return nil
}

func (c *config) Print() {
	fmt.Println(c)
}

func (c *config) GetShadow(storageName string) string {
	return path.Join(c.ClickhouseStorage[storageName], "shadow", strconv.Itoa(c.ShadowDirIncr))
}
