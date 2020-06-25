package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"sync"
)

type connection struct {
	HostName string  `yaml:"hostname"`
	UserName string  `yaml:"username,omitempty"`
	Password string  `yaml:"password,omitempty"`
	Port     uint16  `yaml:"port,omitempty"`
	KeyFilename string  `yaml:"key_filename,omitempty"`
}

type backupStorage struct {
	Type       string     `yaml:"type"`
	BackupDir  string     `yaml:"backup_dir"`
	BackupConn connection `yaml:"backup_conn"`
}

type config struct {
	BackupStorage         backupStorage `yaml:"backup_storage"`
	ClickhouseDir         string        `yaml:"clickhouse_dir"`
	ShadowDir             string        `yaml:"-"`
	ClickhouseBackupConn  connection    `yaml:"clickhouse_backup_conn"`
	ClickhouseRestoreConn connection    `yaml:"clickhouse_restore_conn"`
	//backup_filter interface{}    `yaml:"backup_filter"`
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

func (c *config) Read(filename string)(*config)  {
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	return c
}

func (c *config) Print() {
	fmt.Println(c)
}

func (c *config) SetShadow(s string) {
	c.ShadowDir = s
}
