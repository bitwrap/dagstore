package dagstore_test

import (
	"context"
	"github.com/bitwrap/dagstore"
	"github.com/bitwrap/dagstore/codec"
	_db "github.com/bitwrap/dagstore/db"
	"github.com/bitwrap/dagstore/db/txn"
	"github.com/ledisdb/ledisdb/ledis"
	"github.com/sirupsen/logrus"
	"testing"
)

type TestPayload struct {
	Label string
}

func TestDagstore(t *testing.T) {
	l, err := ledis.Open(_db.Config())
	defer func() { l.Close() }()
	ldb, _ := l.Select(0)

	if err != nil {
		t.Fatalf("%s", err)
	}
	dagstore.Dag.Storage = &_db.DB{DB: ldb}
	dagstore.Dag.State = make([]*codec.Oid, 4)

	testLabel := "foobar"

	dagstore.Bind("foo", func(ctx context.Context, event dagstore.Event, entry *logrus.Entry, txn *txn.Txn) (context.Context, bool) {
		p, ok := ctx.Value("payload").(TestPayload)
		if !ok {
			t.Fatal("failed to cast payload")
		}

		if p.Label != testLabel {
			t.Fatalf("Expected: %s found: %s", testLabel, p.Label)
		}

		t.Logf("Oid: %v, Payload: %v", event.Oid, event.Payload)

		packedOid := codec.ToOid(codec.Marshal(event))
		event.Oid = codec.Oid{}
		unpackedOid := codec.ToOid(codec.Marshal(event))
		if packedOid != unpackedOid {
			t.Logf("packed: %v, unpacked: %v, eventId: %v", packedOid, unpackedOid, event.Oid)
		}

		return ctx, true
	})

	command := func(action string) {
		oid, depth, epoch := dagstore.Commit("foo", "bar", *codec.ToOid([]byte("user")), TestPayload{Label: testLabel})
		t.Logf("oid: %v, depth: %v, epoch: %v", oid.String(), depth, epoch)
	}

	command("foo")
	command("foo")
}
