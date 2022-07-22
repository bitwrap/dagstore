package txn_test

import (
	"github.com/bitwrap/dagstore/codec"
	"github.com/bitwrap/dagstore/db"
	"github.com/bitwrap/dagstore/db/txn"
	"github.com/ledisdb/ledisdb/ledis"
	"testing"
)

var (
	schema    = "test"
	expectVal = []byte("bar")
	testKey   = []byte("foo")
	testField = []byte("attrib")
)

func openDB() (l *ledis.Ledis, ldb *ledis.DB, tx *txn.Txn) {
	l, _ = ledis.Open(db.Config())
	ldb, _ = l.Select(0)
	return l, ldb, txn.New(schema, &db.DB{DB: ldb})
}

func TestTransaction_Cat(t *testing.T) {
	encodedKey := codec.Cat("test", testKey)
	if string(encodedKey) != "zb2rhe3jHdet3jjZyxpc5tKD6DyGY6om1uY8gizGxKwJupRLg" {
		t.Fatal("key encoding failed")
	}
}

func TestTransaction_Set(t *testing.T) {
	l, _, txn := openDB()
	defer func() { l.Close() }()
	err := txn.Set(testKey, expectVal)
	if err != nil {
		t.Fatalf("%v", err)
	}
	v, _ := txn.Get([]byte("foo"))
	if string(v) != string(expectVal) {
		t.Fatalf("%s != %s", v, expectVal)
	}
}

func TestTransaction_HSet(t *testing.T) {
	l, _, txn := openDB()
	defer func() { l.Close() }()

	changes, err := txn.HSet(testKey, testField, expectVal)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if changes != 0 {
		t.Fatal("expected return code 0")
	}

	out, _ := txn.HGet(testKey, testField)

	if string(out) != string(expectVal) {
		t.Fatalf("#{v} != #{expectVal}")
	}
}
