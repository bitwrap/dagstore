package db

import (
	"github.com/bitwrap/dagstore/codec"
	"sync"
)

type Tag struct {
	Root  codec.Oid `json:"root"`
	Epoch uint64    `json:"epoch"`
	Depth uint64    `json:"depth"`
}

type Dag struct {
	Storage *DB
	State   []*codec.Oid
	depth   uint64
	epoch   uint64
	OnEpoch chan Tag
	sync.Mutex
}

type WeldStore = func(cid codec.Oid, data []byte) error

func Weld(a codec.Oid, b codec.Oid, store WeldStore) (id *codec.Oid) {
	data := codec.Cat(Welds, a.Bytes(), b.Bytes())
	id = codec.ToOid(data)
	err := store(*id, data)
	if err != nil {
		panic(err)
	}
	return id
}

func (md *Dag) emitEpoch(root codec.Oid) {
	md.epoch++
	select {
	case md.OnEpoch <- Tag{root, md.epoch, md.depth}:
	default:
		panic("emitEpoch failed")
	}
	md.depth = 0
}

// Append adds an item to the dag, and will emit an Epoch when dag state is full
func (md *Dag) Append(eventId codec.Oid, weldStore func(cid codec.Oid, data []byte) error) (depth uint64, epoch uint64) {
	var carry = &eventId
	inserted := false
	for i, cur := range md.State {
		if cur != nil && cur.Defined() {
			carry = Weld(*cur, eventId, weldStore)
			md.State[i] = nil
		} else {
			md.State[i] = carry
			inserted = true
			break
		}
	}
	if !inserted {
		depth, epoch = md.Append(*carry, weldStore)
		md.emitEpoch(*carry)
	} else {
		md.depth++
	}
	return md.depth, md.epoch
}

// Tag seals the dag and resets the state
func (md *Dag) Tag(store WeldStore) Tag {
	var carry *codec.Oid = nil
	for i := len(md.State) - 1; i >= 0; i-- {
		if md.State[i] != nil && md.State[i].Defined() {
			if carry == nil || !carry.Defined() {
				carry = md.State[i]
			} else {
				carry = Weld(*md.State[i], *carry, store)
			}
			md.State[i] = nil
		}
	}
	if carry == nil {
		panic("attempted to truncate empty dag")
	}
	l := md.depth
	md.emitEpoch(*carry)
	return Tag{*carry, md.epoch, l}
}

// Head generates a root cid for current state
func (md *Dag) Head() *Tag {
	var carry *codec.Oid = nil
	for i := len(md.State) - 1; i >= 0; i-- {
		if md.State[i] != nil && md.State[i].Defined() {
			if carry == nil || !carry.Defined() {
				carry = md.State[i]
			} else {
				carry = Weld(*md.State[i], *carry, func(cid codec.Oid, data []byte) error {
					return nil
				})
			}
		}
	}
	if carry == nil {
		return &Tag{Root: *codec.ToOid([]byte("")), Epoch: 0, Depth: md.depth}
	}
	return &Tag{*carry, md.epoch, md.depth}
}
