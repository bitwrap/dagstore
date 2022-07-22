package txn

// SAdd adds the value to the set.
func (t *Txn) SAdd(key []byte, args ...[]byte) (int64, error) {
	return t.db.SAdd(t.Cat(key), args...)
}

// SCard gets the size of set.
func (t *Txn) SCard(key []byte) (int64, error) {
	return t.db.SCard(t.Cat(key))
}

// SDiff gets the difference of sets.
func (t *Txn) SDiff(keys ...[]byte) ([][]byte, error) {
	return t.db.SDiff(t.encodedKeys(keys)...)
}

// SDiffStore gets the difference of sets and stores to dest set.
func (t *Txn) SDiffStore(dstKey []byte, keys ...[]byte) (int64, error) {
	return t.db.SDiffStore(t.Cat(dstKey), t.encodedKeys(keys)...)
}

// SKeyExists checks whether set existed or not.
func (t *Txn) SKeyExists(key []byte) (int64, error) {
	return t.db.SKeyExists(t.Cat(key))
}

// SInter intersects the sets.
func (t *Txn) SInter(keys ...[]byte) ([][]byte, error) {
	return t.db.SInter(t.encodedKeys(keys)...)
}

// SInterStore intersects the sets and stores to dest set.
func (t *Txn) SInterStore(dstKey []byte, keys ...[]byte) (int64, error) {
	return t.db.SInterStore(t.Cat(dstKey), t.encodedKeys(keys)...)
}

// SIsMember checks member in set.
func (t *Txn) SIsMember(key []byte, member []byte) (int64, error) {
	return t.db.SIsMember(t.Cat(key), member)
}

// SMembers gets members of set.
func (t *Txn) SMembers(key []byte) ([][]byte, error) {
	return t.db.SMembers(t.Cat(key))
}

// SRem removes the members of set.
func (t *Txn) SRem(key []byte, args ...[]byte) (int64, error) {
	return t.db.SRem(t.Cat(key), args...)
}

// SUnion unions the sets.
func (t *Txn) SUnion(keys ...[]byte) ([][]byte, error) {
	return t.db.SUnion(t.encodedKeys(keys)...)
}

// SUnionStore unions the sets and stores to the dest set.
func (t *Txn) SUnionStore(dstKey []byte, keys ...[]byte) (int64, error) {
	return t.db.SUnionStore(t.Cat(dstKey), t.encodedKeys(keys)...)
}

// SClear clears the set.
func (t *Txn) SClear(key []byte) (int64, error) {
	return t.db.SClear(t.Cat(key))
}

// SMclear clears multi sets.
func (t *Txn) SMclear(keys ...[]byte) (int64, error) {
	return t.db.SMclear(t.encodedKeys(keys)...)
}

// SExpire expires the set.
func (t *Txn) SExpire(key []byte, duration int64) (int64, error) {
	return t.db.SExpire(t.Cat(key), duration)
}

// SExpireAt expires the set at when.
func (t *Txn) SExpireAt(key []byte, when int64) (int64, error) {
	return t.db.SExpireAt(t.Cat(key), when)
}

// STTL gets the TTL of set.
func (t *Txn) STTL(key []byte) (int64, error) {
	return t.db.STTL(t.Cat(key))
}

// SPersist removes the TTL of set.
func (t *Txn) SPersist(key []byte) (int64, error) {
	return t.db.SPersist(t.Cat(key))
}
