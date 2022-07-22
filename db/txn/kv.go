package txn

import (
	"github.com/ledisdb/ledisdb/ledis"
	"github.com/ledisdb/ledisdb/store"
)

// Decr decreases the data.
func (t *Txn) Decr(key []byte) (int64, error) {
	return t.db.Decr(t.Cat(key))
}

// DecrBy decreases the data by decrement.
func (t *Txn) DecrBy(key []byte, decrement int64) (int64, error) {
	return t.db.DecrBy(t.Cat(key), decrement)
}

// Del deletes the data.
func (t *Txn) Del(keys ...[]byte) (int64, error) {
	return t.db.Del(t.encodedKeys(keys)...)
}

// Exists check data exists or not.
func (t *Txn) Exists(key []byte) (int64, error) {
	return t.db.Exists(t.Cat(key))
}

// Get gets the value.
func (t *Txn) Get(key []byte) ([]byte, error) {
	return t.db.Get(t.Cat(key))
}

// GetSlice gets the slice of the data.
func (t *Txn) GetSlice(key []byte) (store.Slice, error) {
	return t.db.GetSlice(t.Cat(key))
}

// GetSet gets the value and sets new value.
func (t *Txn) GetSet(key []byte, value []byte) ([]byte, error) {
	return t.db.GetSet(t.Cat(key), value)
}

// Incr increases the data.
func (t *Txn) Incr(key []byte) (int64, error) {
	return t.db.Incr(t.Cat(key))
}

// IncrBy increases the data by increment.
func (t *Txn) IncrBy(key []byte, increment int64) (int64, error) {
	return t.db.IncrBy(t.Cat(key), increment)
}

// MGet gets multi data.
func (t *Txn) MGet(keys ...[]byte) ([][]byte, error) {
	return t.db.MGet(t.encodedKeys(keys)...)
}

// MSet sets multi data.
func (t *Txn) MSet(args ...ledis.KVPair) error {
	return t.db.MSet(t.encodedKVPairs(args)...)
}

// Set sets the data.
func (t *Txn) Set(key []byte, value []byte) error {
	return t.db.Set(t.Cat(key), value)
}

// SetNX sets the data if not existed.
func (t *Txn) SetNX(key []byte, value []byte) (int64, error) {
	return t.db.SetNX(t.Cat(key), value)
}

// SetEX sets the data with a TTL.
func (t *Txn) SetEX(key []byte, duration int64, value []byte) error {
	return t.db.SetEX(t.Cat(key), duration, value)
}

// Expire expires the data.
func (t *Txn) Expire(key []byte, duration int64) (int64, error) {
	return t.db.Expire(t.Cat(key), duration)
}

// ExpireAt expires the data at when.
func (t *Txn) ExpireAt(key []byte, when int64) (int64, error) {
	return t.db.ExpireAt(t.Cat(key), when)
}

// TTL returns the TTL of the data.
func (t *Txn) TTL(key []byte) (int64, error) {
	return t.db.TTL(t.Cat(key))
}

// Persist removes the TTL of the data.
func (t *Txn) Persist(key []byte) (int64, error) {
	return t.db.Persist(t.Cat(key))
}

// SetRange sets the data with new value from offset.
func (t *Txn) SetRange(key []byte, offset int, value []byte) (int64, error) {
	return t.db.SetRange(t.Cat(key), offset, value)
}

// GetRange gets the range of the data.
func (t *Txn) GetRange(key []byte, start int, end int) ([]byte, error) {
	return t.db.GetRange(t.Cat(key), start, end)
}

// StrLen returns the length of the data.
func (t *Txn) StrLen(key []byte) (int64, error) {
	return t.db.StrLen(t.Cat(key))
}

// Append appends the value to the data.
func (t *Txn) Append(key []byte, value []byte) (int64, error) {
	return t.db.Append(t.Cat(key), value)
}

// BitOP does the bit operations in data.
func (t *Txn) BitOP(op string, destKey []byte, srcKeys ...[]byte) (int64, error) {
	return t.db.BitOP(op, t.Cat(destKey), t.encodedKeys(srcKeys)...)
}

// BitCount returns the bit count of data.
func (t *Txn) BitCount(key []byte, start int, end int) (int64, error) {
	return t.db.BitCount(t.Cat(key), start, end)
}

// BitPos returns the pos of the data.
func (t *Txn) BitPos(key []byte, on int, start int, end int) (int64, error) {
	return t.db.BitPos(t.Cat(key), on, start, end)
}

// SetBit sets the bit to the data.
func (t *Txn) SetBit(key []byte, offset int, on int) (int64, error) {
	return t.db.SetBit(t.Cat(key), offset, on)
}

// GetBit gets the bit of data at offset.
func (t *Txn) GetBit(key []byte, offset int) (int64, error) {
	return t.db.GetBit(t.Cat(key), offset)
}
