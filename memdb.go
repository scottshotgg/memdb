package memdb

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type DB interface {
	Name() string
	Location() string

	Write() error

	Upsert(key string, value []byte)
	Retrieve(key string) []byte
	Delete(key string)

	CreateBucket(key string) error
	GetBucket(key string) DB
	DeleteBucket(key string)

	Explode() error
}

type MemDB struct {
	name     string
	location string
	store    map[string][]byte
	children map[string]DB
}

// New makes a new DB; this is also a bucket
func New(name, location string) (DB, error) {
	return &MemDB{
		name:     name,
		location: location,
		store:    map[string][]byte{},
		children: map[string]DB{},
	}, nil
}

// Open allows reading a bucket from a file
func Open(location string) (DB, error) {
	var contents, err = ioutil.ReadFile(location)
	if err != nil {
		return nil, err
	}

	var db MemDB

	return &db, json.Unmarshal(contents, &db)
}

// UnmarshalJSON is the unmarshaller for JSON format
func (db *MemDB) UnmarshalJSON(b []byte) error {
	var db2 = struct {
		Name     string
		Location string
		Store    map[string][]byte
		Children map[string]*MemDB
	}{
		"",
		"",
		map[string][]byte{},
		map[string]*MemDB{},
	}

	var err = json.Unmarshal(b, &db2)
	if err != nil {
		return nil
	}

	db.name = db2.Name
	db.location = db2.Location
	db.store = db2.Store
	db.children = map[string]DB{}

	for key, value := range db2.Children {
		db.children[key] = value
	}

	return nil
}

// MarshalJSON is the marshaller for JSON format
func (db MemDB) MarshalJSON() ([]byte, error) {
	var j, err = json.Marshal(struct {
		Name     string
		Location string
		Store    map[string][]byte
		Children map[string]DB
	}{
		db.name,
		db.location,
		db.store,
		db.children,
	})
	if err != nil {
		return nil, err
	}
	return j, nil
}

// Name returns the name of the DB
func (db *MemDB) Name() string {
	return db.name
}

// Location will give you back the location of the file
func (db *MemDB) Location() string {
	return db.location
}

// Upsert updates or inserts a key into a DB
func (db *MemDB) Upsert(key string, value []byte) {
	db.store[key] = value
}

// Retrieve fetches a value from the DB by key
func (db *MemDB) Retrieve(key string) []byte {
	return db.store[key]
}

// Delete removes a value from the DB by key
func (db *MemDB) Delete(key string) {
	delete(db.store, key)
}

// CreateBucket adds a new child DB to an existing DB
func (db *MemDB) CreateBucket(key string) (err error) {
	if db.children[key] != nil {
		return ErrAlreadyExists
	}

	db.children[key], err = New(key, "")

	return err
}

// GetBucket returns a bucket for use
func (db *MemDB) GetBucket(key string) DB {
	return db.children[key]
}

// DeleteBucket removes a bucket entirely from the DB
func (db *MemDB) DeleteBucket(key string) {
	delete(db.children, key)
}

// Write dumps the database to the location file
// This only works on the top level bucket
func (db *MemDB) Write() error {
	if db.location == "" {
		return ErrNonRootBucket
	}

	var contents, err = json.Marshal(db)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(db.location, contents, 0666)
}

func (db *MemDB) SetMarshaller(m func(db *MemDB) ([]byte, error)) error {
	return ErrNotImplemented
}

func (db *MemDB) SetUnmarshaller(u func([]byte) error) error {
	return ErrNotImplemented
}

func (db *MemDB) Close() {}

func (db *MemDB) Explode() error {
	return os.Remove(db.location)
}
