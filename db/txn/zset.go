package txn

import (
	"github.com/bitwrap/dagstore/codec"
	"github.com/ledisdb/ledisdb/ledis"
)

// ZAdd add the members.
func (t *Txn) ZAdd(key []byte, args ...ledis.ScorePair) (int64, error) {
	return t.db.ZAdd(codec.Cat(t.Schema, key), args...)
}

// ZCard gets the size of the zset.
func (t *Txn) ZCard(key []byte) (int64, error) {
	return t.db.ZCard(codec.Cat(t.Schema, key))
}

// ZScore gets the score of member.
func (t *Txn) ZScore(key []byte, member []byte) (int64, error) {
	return t.db.ZScore(codec.Cat(t.Schema, key), member)
}

// ZRem removes members
func (t *Txn) ZRem(key []byte, members ...[]byte) (int64, error) {
	return t.db.ZRem(codec.Cat(t.Schema, key), members...)
}

// ZIncrBy increases the score of member with delta.
func (t *Txn) ZIncrBy(key []byte, delta int64, member []byte) (int64, error) {
	return t.db.ZIncrBy(codec.Cat(t.Schema, key), delta, member)
}

// ZCount gets the number of score in [min, max]
func (t *Txn) ZCount(key []byte, min int64, max int64) (int64, error) {
	return t.db.ZCount(codec.Cat(t.Schema, key), min, max)
}

// ZClear clears the zset.
func (t *Txn) ZClear(key []byte) (int64, error) {
	return t.db.ZClear(codec.Cat(t.Schema, key))
}

// ZMclear clears multi zsets.
func (t *Txn) ZMclear(keys ...[]byte) (int64, error) {
	return t.db.ZMclear(t.encodedKeys(keys)...)
}

// ZRange gets the members from start to stop.
func (t *Txn) ZRange(key []byte, start int, stop int) ([]ledis.ScorePair, error) {
	return t.db.ZRange(codec.Cat(t.Schema, key), start, stop)
}

// ZRangeByScore gets the data with score in min and max.
// min and max must be inclusive
// if no limit, set offset = 0 and count = -1
func (t *Txn) ZRangeByScore(key []byte, min int64, max int64, offset int, count int) ([]ledis.ScorePair, error) {
	return t.db.ZRangeByScore(codec.Cat(t.Schema, key), min, max, offset, count)
}

// ZRank gets the rank of member.
func (t *Txn) ZRank(key []byte, member []byte) (int64, error) {
	return t.db.ZRank(codec.Cat(t.Schema, key), member)
}

// ZRemRangeByRank removes the member at range from start to stop.
func (t *Txn) ZRemRangeByRank(key []byte, start int, stop int) (int64, error) {
	return t.db.ZRemRangeByRank(codec.Cat(t.Schema, key), start, stop)
}

// ZRemRangeByScore removes the data with score at [min, max]
func (t *Txn) ZRemRangeByScore(key []byte, min int64, max int64) (int64, error) {
	return t.db.ZRemRangeByScore(codec.Cat(t.Schema, key), min, max)
}

// ZRevRange gets the data reversed.
func (t *Txn) ZRevRange(key []byte, start int, stop int) ([]ledis.ScorePair, error) {
	return t.db.ZRevRange(codec.Cat(t.Schema, key), start, stop)
}

// ZRevRank gets the rank of member reversed.
func (t *Txn) ZRevRank(key []byte, member []byte) (int64, error) {
	return t.db.ZRevRank(codec.Cat(t.Schema, key), member)
}

// ZRevRangeByScore gets the data with score at [min, max]
// min and max must be inclusive
// if no limit, set offset = 0 and count = -1
func (t *Txn) ZRevRangeByScore(key []byte, min int64, max int64, offset int, count int) ([]ledis.ScorePair, error) {
	return t.db.ZRevRangeByScore(codec.Cat(t.Schema, key), min, max, offset, count)
}

// ZRangeGeneric is a generic function for scan zset.
func (t *Txn) ZRangeGeneric(key []byte, start int, stop int, reverse bool) ([]ledis.ScorePair, error) {
	return t.db.ZRangeGeneric(codec.Cat(t.Schema, key), start, stop, reverse)
}

// ZRangeByScoreGeneric is a generic function to scan zset with score.
// min and max must be inclusive
// if no limit, set offset = 0 and count = -1
func (t *Txn) ZRangeByScoreGeneric(key []byte, min int64, max int64, offset int, count int, reverse bool) ([]ledis.ScorePair, error) {
	return t.db.ZRangeByScoreGeneric(codec.Cat(t.Schema, key), min, max, offset, count, reverse)
}

// ZExpire expires the zset.
func (t *Txn) ZExpire(key []byte, duration int64) (int64, error) {
	return t.db.ZExpire(codec.Cat(t.Schema, key), duration)
}

// ZExpireAt expires the zset at when.
func (t *Txn) ZExpireAt(key []byte, when int64) (int64, error) {
	return t.db.ZExpireAt(codec.Cat(t.Schema, key), when)
}

// ZTTL gets the TTL of zset.
func (t *Txn) ZTTL(key []byte) (int64, error) {
	return t.db.ZTTL(codec.Cat(t.Schema, key))
}

// ZPersist removes the TTL of zset.
func (t *Txn) ZPersist(key []byte) (int64, error) {
	return t.db.ZPersist(codec.Cat(t.Schema, key))
}

// ZUnionStore unions the zsets and stores to dest zset.
func (t *Txn) ZUnionStore(destKey []byte, srcKeys [][]byte, weights []int64, aggregate byte) (int64, error) {
	return t.db.ZUnionStore(codec.Cat(t.Schema, destKey), t.encodedKeys(srcKeys), weights, aggregate)
}

// ZInterStore intersects the zsets and stores to dest zset.
func (t *Txn) ZInterStore(destKey []byte, srcKeys [][]byte, weights []int64, aggregate byte) (int64, error) {
	return t.db.ZInterStore(codec.Cat(t.Schema, destKey), t.encodedKeys(srcKeys), weights, aggregate)
}

// ZRangeByLex scans the zset lexicographically
func (t *Txn) ZRangeByLex(key []byte, min []byte, max []byte, rangeType uint8, offset int, count int) ([][]byte, error) {
	return t.db.ZRangeByLex(codec.Cat(t.Schema, key), min, max, rangeType, offset, count)
}

// ZRemRangeByLex remvoes members in [min, max] lexicographically
func (t *Txn) ZRemRangeByLex(key []byte, min []byte, max []byte, rangeType uint8) (int64, error) {
	return t.db.ZRemRangeByLex(codec.Cat(t.Schema, key), min, max, rangeType)
}

// ZLexCount gets the count of zset lexicographically.
func (t *Txn) ZLexCount(key []byte, min []byte, max []byte, rangeType uint8) (int64, error) {
	return t.db.ZLexCount(codec.Cat(t.Schema, key), min, max, rangeType)
}

// ZKeyExists checks zset existed or not.
func (t *Txn) ZKeyExists(key []byte) (int64, error) {
	return t.db.ZKeyExists(codec.Cat(t.Schema, key))
}
