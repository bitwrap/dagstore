package txn

import (
	"github.com/ledisdb/ledisdb/ledis"
)

// HLen returns the length of hash.
func (t *Txn) HLen(key []byte) (int64, error) {
	return t.db.HLen(t.Cat(key))
}

// HSet sets the field with value of key.
func (t *Txn) HSet(key []byte, field []byte, value []byte) (int64, error) {
	return t.db.HSet(t.Cat(key), field, value)
}

// HGet gets the value of the field.
func (t *Txn) HGet(key []byte, field []byte) ([]byte, error) {
	return t.db.HGet(t.Cat(key), field)
}

// HMset sets multi field-values.
func (t *Txn) HMset(key []byte, args ...ledis.FVPair) error {
	return t.db.HMset(t.Cat(key), args...)
}

// HMget gets multi values of fields
func (t *Txn) HMget(key []byte, args ...[]byte) ([][]byte, error) {
	return t.db.HMget(t.Cat(key), args...)
}

// HDel deletes the fields.
func (t *Txn) HDel(key []byte, args ...[]byte) (int64, error) {
	return t.db.HDel(t.Cat(key), args...)
}

// HIncrBy increases the value of field by delta.
func (t *Txn) HIncrBy(key []byte, field []byte, delta int64) (int64, error) {
	return t.db.HIncrBy(t.Cat(key), field, delta)
}

// HGetAll returns all field-values.
func (t *Txn) HGetAll(key []byte) ([]ledis.FVPair, error) {
	return t.db.HGetAll(t.Cat(key))
}

// HKeys returns the all fields.
func (t *Txn) HKeys(key []byte) ([][]byte, error) {
	return t.db.HKeys(t.Cat(key))
}

// HValues returns all values
func (t *Txn) HValues(key []byte) ([][]byte, error) {
	return t.db.HValues(t.Cat(key))
}

// HClear clears the data.
func (t *Txn) HClear(key []byte) (int64, error) {
	return t.db.HClear(t.Cat(key))
}

// HMclear cleans multi data.
func (t *Txn) HMclear(keys ...[]byte) (int64, error) {
	return t.db.HMclear(t.encodedKeys(keys)...)
}

// HExpire expires the data with duration.
func (t *Txn) HExpire(key []byte, duration int64) (int64, error) {
	return t.db.HExpire(t.Cat(key), duration)
}

// HExpireAt expires the data at time when.
func (t *Txn) HExpireAt(key []byte, when int64) (int64, error) {
	return t.db.HExpireAt(t.Cat(key), when)
}

// HTTL gets the TTL of data.
func (t *Txn) HTTL(key []byte) (int64, error) {
	return t.db.HTTL(t.Cat(key))
}

// HPersist removes the TTL of data.
func (t *Txn) HPersist(key []byte) (int64, error) {
	return t.db.HPersist(t.Cat(key))
}

// HKeyExists checks whether data exists or not.
func (t *Txn) HKeyExists(key []byte) (int64, error) {
	return t.db.HKeyExists(t.Cat(key))
}
