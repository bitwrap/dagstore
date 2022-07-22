package db_test

import (
	"fmt"
	. "github.com/bitwrap/dagstore/codec"
	_db "github.com/bitwrap/dagstore/db"
	"github.com/ledisdb/ledisdb/ledis"
	"testing"
)

func TestDagStore_Append(t *testing.T) {
	l, err := ledis.Open(_db.Config())
	defer func() { l.Close() }()
	ldb, _ := l.Select(0)

	if err != nil {
		t.Fatalf("%s", err)
	}
	dag := _db.Dag{
		Storage: &_db.DB{DB: ldb},
		State:   make([]*Oid, 4),
		OnEpoch: make(chan _db.Tag),
	}

	fakeWeld := func(cid Oid, data []byte) error {
		// KLUDGE: not storing welds
		return nil
	}

	go func() {
		for {
			select {
			case e := <-dag.OnEpoch:
				_ = e
				t.Logf("epoch: %v", e)
				continue
			}
		}
	}()
	for i := 0; i < 100; i++ {
		depth, epoch := dag.Append(*ToOid([]byte(fmt.Sprintf("foo%v", i))), fakeWeld)
		_ = depth
		_ = epoch
		//t.Logf("%v %v", epoch, depth)
	}

	assertTagComputation := func(tag _db.Tag, expectDepth uint64, expectEpoch uint64, expectedHash string) {
		//t.Logf("%v", tag)
		if tag.Epoch != expectEpoch || tag.Depth != expectDepth {
			t.Fatalf("Unexpected Tag %v", tag)
		}
		root := tag.Root.String()
		if root != expectedHash {
			encodedRoot := tag.Root.String()
			t.Fatalf("val %s != expected %s", encodedRoot, expectedHash)
		}
	}

	_ = assertTagComputation
	/*
		var expectHead = "zb2rhfcRo91bMyyxVkzusYKHNvDh3vAcidPNHESKtd4PpXYE1"
		assertTagComputation(*dag.Head(), uint64(10), uint64(6), expectHead)
		tag := dag.Tag(fakeWeld)
		assertTagComputation(tag, uint64(10), uint64(7), expectHead)
	*/

}
