package database

import (
	"fmt"
	"log"
	"testing"
)

func TestGetDBS(t *testing.T) {
	ch:=New()
	ch.SetDSN("tcp://centos08:9000")
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
	ch.SetDSN("tcp://centos08:9000")
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
	ch.SetDSN("tcp://centos08:9000")
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
	ch.SetDSN("tcp://centos08:9000")
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
	ch.SetDSN("tcp://centos08:9000")
	err:=ch.FreezeTable("default",".inner.visits_and_registrations","")
	if err != nil {
		t.Error("Error freeze table", err)
	}
	err=ch.FreezeTable("default",".inner.visits_and_registrations","1970-08-22")
	if err != nil {
		t.Error("Error freeze table", err)
	}

}