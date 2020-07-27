package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"sync"
)

type Connection struct {
	HostName string  `yaml:"hostname"`
	UserName string  `yaml:"username,omitempty"`
	Password string  `yaml:"password,omitempty"`
	Port     uint16  `yaml:"port,omitempty"`
	KeyFilename string  `yaml:"key_filename,omitempty"`
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

type taskargs struct {
	JobType	   RunJobType
	JobName    string
	JobPartition string
	Debug bool
}

type ChMetaOpts struct {
	CutReplicated bool `yaml:"replace_replicated_to_default"`
}

type config struct {
	BackupStorage         backupStorage       `yaml:"backup_storage"`
	ClickhouseDir         string              `yaml:"clickhouse_dir"`
	ShadowDir             string              `yaml:"-"`
	TaskArgs              taskargs            `yaml:"-"`
	ClickhouseBackupConn  Connection          `yaml:"clickhouse_backup_conn"`
	ClickhouseRestoreConn Connection          `yaml:"clickhouse_restore_conn"`
	ClickhouseRestoreOpts ChMetaOpts          `yaml:"clickhouse_restore_opts"`
	BackupFilter          map[string][]string `yaml:"backup_filter"`
}

var (
	once sync.Once
	instance *config
)

func New() *config {
	once.Do(func() {
		instance = new(config)
	})
	return instance
}

func (c *config) Read(filename string)(error)  {
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

func (c *config) SetShadow(s string) {
	c.ShadowDir = s
}
