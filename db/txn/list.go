package txn

import (
	"time"
)

// LIndex returns the value at index.
func (t *Txn) LIndex(key []byte, index int32) ([]byte, error) {
	return t.db.LIndex(t.Cat(key), index)
}

// LLen gets the length of the list.
func (t *Txn) LLen(key []byte) (int64, error) {
	return t.db.LLen(t.Cat(key))
}

// LPop pops the value.
func (t *Txn) LPop(key []byte) ([]byte, error) {
	return t.db.LPop(t.Cat(key))
}

// LTrim trims the value from start to stop.
func (t *Txn) LTrim(key []byte, start, stop int64) error {
	return t.db.LTrim(t.Cat(key), start, stop)
}

// LTrimFront trims the value from top.
func (t *Txn) LTrimFront(key []byte, trimSize int32) (int32, error) {
	return t.db.LTrimFront(t.Cat(key), trimSize)
}

// LTrimBack trims the value from back.
func (t *Txn) LTrimBack(key []byte, trimSize int32) (int32, error) {
	return t.db.LTrimBack(t.Cat(key), trimSize)
}

// LPush push the value to the list.
func (t *Txn) LPush(key []byte, args ...[]byte) (int64, error) {
	return t.db.LPush(t.Cat(key), args...)
}

// LSet sets the value at index.
func (t *Txn) LSet(key []byte, index int32, value []byte) error {
	return t.db.LSet(t.Cat(key), index, value)
}

// LRange gets the value of list at range.
func (t *Txn) LRange(key []byte, start int32, stop int32) ([][]byte, error) {
	return t.db.LRange(t.Cat(key), start, stop)
}

// RPop rpops the value.
func (t *Txn) RPop(key []byte) ([]byte, error) {
	return t.db.RPop(t.Cat(key))
}

// RPush rpushs the value.
func (t *Txn) RPush(key []byte, args ...[]byte) (int64, error) {
	return t.db.RPush(t.Cat(key), args...)
}

// LClear clears the list.
func (t *Txn) LClear(key []byte) (int64, error) {
	return t.db.LClear(t.Cat(key))
}

// LMclear clears multi lists.
func (t *Txn) LMclear(keys ...[]byte) (int64, error) {
	return t.db.LMclear(t.encodedKeys(keys)...)
}

// LExpire expires the list.
func (t *Txn) LExpire(key []byte, duration int64) (int64, error) {
	return t.db.LExpire(t.Cat(key), duration)
}

// LExpireAt expires the list at when.
func (t *Txn) LExpireAt(key []byte, when int64) (int64, error) {
	return t.db.LExpireAt(t.Cat(key), when)
}

// LTTL gets the TTL of list.
func (t *Txn) LTTL(key []byte) (int64, error) {
	return t.db.LTTL(t.Cat(key))
}

// LPersist removes the TTL of list.
func (t *Txn) LPersist(key []byte) (int64, error) {
	return t.db.LPersist(t.Cat(key))
}

// BLPop pops the list with block way.
func (t *Txn) BLPop(keys [][]byte, timeout time.Duration) ([]interface{}, error) {
	return t.db.BLPop(t.encodedKeys(keys), timeout)
}

// BRPop bpops the list with block way.
func (t *Txn) BRPop(keys [][]byte, timeout time.Duration) ([]interface{}, error) {
	return t.db.BRPop(t.encodedKeys(keys), timeout)
}

// LKeyExists check list existed or not.
func (t *Txn) LKeyExists(key []byte) (int64, error) {
	return t.db.LKeyExists(t.Cat(key))
}
