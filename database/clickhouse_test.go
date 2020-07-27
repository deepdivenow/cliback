package database

import (
	"cliback/config"
	"fmt"
	"log"
	"testing"
)

var (
	testDSN = config.Connection{
		HostName:    "centos08",
		UserName:    "default",
		Password:    "",
		Port:        9000,
	}
)

func TestGetDBS(t *testing.T) {
	ch:=New()
	ch.SetDSN(testDSN)
	getdbs,err:=ch.GetDBS()
	if err == nil {
		if len(getdbs) < 1 {
			t.Error("Number DBS must be more then zero", getdbs)
		} else {
			log.Printf("Databases: %s\n",getdbs)
		}
	} else {
		t.Error("Fail connect to database", err)
	}
}

func TestGetTables(t *testing.T) {
	ch:=New()
	ch.SetDSN(testDSN)
	tables,err:=ch.GetTables("default")
	if err == nil {
		if len(tables) < 1 {
			t.Error("Number Tables must be more then zero", tables)
		} else {
			fmt.Printf("Tables: %s\n",tables)
		}
	} else {
		t.Error("Fail connect to database", err)
	}
}

func TestGetPartitions(t *testing.T) {
	ch:=New()
	ch.SetDSN(testDSN)
	part,err:=ch.GetPartitions("default",".inner.visits_and_registrations","")
	if err == nil {
		if len(part) < 1 {
			t.Error("Number Partitions must be more then zero", part)
		} else {
			fmt.Printf("Partitions: %s\n", part)
		}
	} else {
		t.Error("Fail connect to database", err)
	}
}

func TestGetFNames(t *testing.T) {
	ch:=New()
	ch.SetDSN(testDSN)
	part,err:=ch.GetFNames("default",".inner.visits_and_registrations","")
	if err == nil {
		if len(part) < 1 {
			t.Error("Number Partitions must be more then zero", part)
		} else {
			fmt.Printf("Partitions: %s\n", part)
		}
	} else {
		t.Error("Fail connect to database", err)
	}
}

func TestFreezeTable(t *testing.T) {
	ch:=New()
	ch.SetDSN(testDSN)
	err:=ch.FreezeTable("default",".inner.visits_and_registrations","")
	if err != nil {
		t.Error("Error freeze table", err)
	}
	err=ch.FreezeTable("default",".inner.visits_and_registrations","1970-08-22")
	if err != nil {
		t.Error("Error freeze table", err)
	}

}
func TestReplaceReplicatedMeta(t *testing.T) {
	meta:="CREATE TABLE IF NOT EXISTS `analytics`.`google_analytics_split_event`\n(\n`domain` String,\n     `created_at` DateTime,\n     `user_id` Nullable(String),\n     `referrer` Nullable(String),\n     `target` Nullable(String),\n     `ga_id` Nullable(String),\n     `device` Nullable(String),\n     `utm_source` Nullable(String),\n     `utm_campaign` Nullable(String),\n     `utm_medium` Nullable(String),\n     `utm_content` Nullable(String),\n     `experiments` Nullable(String),\n     `_header_luna_id` String,\n     `_date` Date\n )\n ENGINE = ReplicatedMergeTree('/var/lib/clickhouse/first/analytics.google_analytics_split_event', '{replica}')\n PARTITION BY toYear(created_at)\n ORDER BY created_at\n SETTINGS index_granularity = 8192"
	meta_expect:="CREATE TABLE IF NOT EXISTS `analytics`.`google_analytics_split_event`\n(\n`domain` String,\n     `created_at` DateTime,\n     `user_id` Nullable(String),\n     `referrer` Nullable(String),\n     `target` Nullable(String),\n     `ga_id` Nullable(String),\n     `device` Nullable(String),\n     `utm_source` Nullable(String),\n     `utm_campaign` Nullable(String),\n     `utm_medium` Nullable(String),\n     `utm_content` Nullable(String),\n     `experiments` Nullable(String),\n     `_header_luna_id` String,\n     `_date` Date\n )\n ENGINE = MergeTree()\n PARTITION BY toYear(created_at)\n ORDER BY created_at\n SETTINGS index_granularity = 8192"
	meta=ReplaceCutReplicatedTable(meta)
	if meta != meta_expect{
		t.Error("Meta replicationMergeTree BAD replace")
	}
}
func TestReplaceReplicatedMetav2(t *testing.T) {
	meta:="ATTACH TABLE visit\n(\n`target` String,\n`ga_id` String,\n`campaign_id` Int32,\n`ip` String,\n`referrer` String,\n`datetime` DateTime,\n`type` String,\n`request_id` UUID,\n`partner_id` Int32,\n`manager_id` Int32,\n`date` Date\n)\nENGINE = ReplicatedMergeTree('/var/lib/clickhouse/first/visit', '{replica}', date, (request_id, date), 8192)"
	meta_expect:="ATTACH TABLE visit\n(\n`target` String,\n`ga_id` String,\n`campaign_id` Int32,\n`ip` String,\n`referrer` String,\n`datetime` DateTime,\n`type` String,\n`request_id` UUID,\n`partner_id` Int32,\n`manager_id` Int32,\n`date` Date\n)\nENGINE = MergeTree(date,(request_id,date),8192)"
	meta=ReplaceCutReplicatedTable(meta)
	if meta != meta_expect{
		t.Error("Meta replicationMergeTree BAD replace")
	}
}