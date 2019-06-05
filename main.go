package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
)

var (
	ErrAlreadyExists = errors.New("Key already exists")
)

type DB interface {
	Name() string
	Location() string

	Insert(key string, value []byte) error
	Retrieve(key string) []byte
	Delete(key string)

	CreateBucket(key string) error
	Bucket(key string) DB

	Write() error
}

type MemDB struct {
	name     string
	location string
	store    map[string][]byte
	children map[string]DB
}

func (db *MemDB) Bucket(key string) DB {
	return db.children[key]
}

func (db *MemDB) Location() string {
	return db.location
}

func (db *MemDB) Name() string {
	return db.name
}

func New(name, location string) (DB, error) {
	return &MemDB{
		name:     name,
		location: location,
		store:    map[string][]byte{},
		children: map[string]DB{},
	}, nil
}

func Open(location string) (DB, error) {
	var contents, err = ioutil.ReadFile(location)
	if err != nil {
		return nil, err
	}

	var db MemDB

	return &db, json.Unmarshal(contents, &db)
}

func (db *MemDB) Delete(key string) {
	delete(db.store, key)
}

func (db *MemDB) CreateBucket(key string) error {
	var b = db.children[key]
	if b != nil {
		return ErrAlreadyExists
	}

	var err error

	db.children[key], err = New(key, key+".db")

	return err
}

func (db *MemDB) Retrieve(key string) []byte {
	return db.store[key]
}

func (db *MemDB) Insert(key string, value []byte) error {
	if db.Retrieve("key") != nil {
		return ErrAlreadyExists
	}

	db.store[key] = value

	return nil
}

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

func (db *MemDB) Write() error {
	var contents, err = json.Marshal(db)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(db.location, contents, 0666)
}

func main() {
	var db DB

	db = &MemDB{
		name:     "test",
		location: "test.db",
		store:    map[string][]byte{},
		children: map[string]DB{},
	}

	var err = db.Insert("something", []byte("sodikjfaldsjf"))
	if err != nil {
		fmt.Println("err", err)
		return
	}

	var value = db.Retrieve("something")
	if value == nil {
		fmt.Println("Value was not there")
		return
	}

	fmt.Println("value", string(value))

	err = db.CreateBucket("test2")
	if err != nil {
		fmt.Println("err", err)
		return
	}

	var b = db.Bucket("test2")
	if b == nil {
		fmt.Println("Bucket was not there")
		return
	}

	err = b.Insert("something2", []byte("sodikjfaldsjf2"))
	if err != nil {
		fmt.Println("err", err)
		return
	}

	var b2 = db.Bucket("test2")
	if b2 == nil {
		fmt.Println("Bucket was not there")
		return
	}

	fmt.Println("something2:", string(b2.Retrieve("something2")))

	err = db.Write()
	if err != nil {
		fmt.Println("error writing", err)
		return
	}

	db, err = Open("test.db")
	if err != nil {
		fmt.Println("err", err)

		return
	}

	fmt.Printf("db after opening: %+v", db)
}
