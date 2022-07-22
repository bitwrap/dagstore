package txn

import (
	"github.com/bitwrap/dagstore/codec"
	"github.com/bitwrap/dagstore/db"
	"github.com/ledisdb/ledisdb/ledis"
)

type Txn struct {
	Schema string
	db     *db.DB
}

func New(schema string, db *db.DB) *Txn {
	t := new(Txn)
	t.Schema = schema
	t.db = db
	return t
}

// Cat prepends schema and encodes key with base58btc encoder
func (t *Txn) Cat(b ...[]byte) []byte {
	return codec.Cat(t.Schema, b...)
}

// Sub extends schema by appending a sub-schema
func (t *Txn) Sub(schema string) *Txn {
	n := new(Txn)
	n.Schema = t.Schema + "/" + schema
	n.db = t.db
	return n
}

// encodeKeys adds Cat encoding to a set of keys
func (t *Txn) encodedKeys(keys [][]byte) [][]byte {
	hkeys := make([][]byte, len(keys))
	for _, key := range keys {
		hkeys = append(hkeys, codec.Cat(t.Schema, key))
	}
	return hkeys
}

// encodedKVPair adds Cat encoding to a KVPair
func (t *Txn) encodedKVPair(pair ledis.KVPair) ledis.KVPair {
	return ledis.KVPair{
		Key:   codec.Cat(t.Schema, pair.Key),
		Value: pair.Value,
	}
}

// encodedKVPair adds Cat encoding to KVPairs
func (t *Txn) encodedKVPairs(pairs []ledis.KVPair) []ledis.KVPair {
	newPairs := make([]ledis.KVPair, len(pairs))
	for i, pair := range pairs {
		newPairs[i] = t.encodedKVPair(pair)
	}
	return newPairs
}
