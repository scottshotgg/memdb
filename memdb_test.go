package memdb_test

import (
	"fmt"
	"testing"

	"github.com/scottshotgg/memdb"
)

var (
	dbName = "test"
	db     memdb.DB
	err    error
)

func TestNew(t *testing.T) {
	db, err = memdb.New(dbName, dbName+".db")
	if err != nil {
		t.Fatalf("err: %+v", err)
	}
}

func TestOpen(t *testing.T) {
	db, err = memdb.Open(dbName + ".db")
	if err != nil {
		t.Fatalf("err: %+v", err)
	}

	fmt.Printf("%+v", db)
}

func TestUpsert(t *testing.T) {
	TestNew(t)

	db.Upsert("test_key", []byte("test_value"))
}

func TestRetrieve(t *testing.T) {
	TestUpsert(t)

	var v = db.Retrieve("test_key")
	if v == nil {
		t.Fatalf("v: %+v", v)
	}

	t.Logf("v: %s", v)
}

func TestWrite(t *testing.T) {
	TestRetrieve(t)

	err = db.Write()
	if err != nil {
		t.Fatalf("err: %+v", err)
	}
}

func TestReOpen(t *testing.T) {
	TestWrite(t)

	TestOpen(t)

	t.Logf("DB: %+v\n", db)
}

func TestExplode(t *testing.T) {
	TestReOpen(t)

	err = db.Explode()
	if err != nil {
		t.Fatalf("err: %+v", err)
	}
}
