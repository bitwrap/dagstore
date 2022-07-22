package dagstore

import (
	"context"
	"errors"
	"github.com/bitwrap/dagstore/codec"
	"github.com/bitwrap/dagstore/db"
	"github.com/bitwrap/dagstore/db/txn"
	"github.com/sirupsen/logrus"
	"strconv"
)

type Event struct {
	Schema  string
	Action  string
	Actor   codec.Oid
	Payload codec.Oid
	Commit  codec.Oid
	Epoch   uint64
	Depth   uint64
	Oid     codec.Oid
}

type EventHandler = func(context.Context, Event, *logrus.Entry, *txn.Txn) (ctx context.Context, ok bool)

var (
	eventMap = make(map[string]EventHandler, 0)
	Logger   = logrus.New()
	Dag      = new(db.Dag)
)

// Dispatch invokes all registered handlers for an event
func Dispatch(event Event, payload ...interface{}) (err error) {
	ctx := context.Background()
	if len(payload) == 1 {
		ctx = context.WithValue(ctx, "payload", payload[0])
	}

	handle, found := eventMap[event.Schema]
	logEntry := Logger.WithFields(map[string]interface{}{"schema": event.Schema})
	if found {
		_, ok := handle(ctx, event, logEntry, txn.New(event.Schema, Dag.Storage))
		// REVIEW: do something with ctx here?
		if !ok {
			err = errors.New("handler failed")
		}
	} else {
		logEntry.Error("dagCommit failed")
		err = errors.New("dagCommit failed")
	}
	return err
}

const Epoch = "Epoch"

var kernel = codec.ToOid([]byte("kernel"))

func ProcessEpochs() {
	Bind(Epoch, func(ctx context.Context, e Event, _ *logrus.Entry, t *txn.Txn) (context.Context, bool) {
		if e.Actor != *kernel {
			return ctx, false
		}
		key := []byte(strconv.FormatUint(e.Epoch, 10))
		tag := ctx.Value("payload").(db.Tag)
		err := t.Set(key, codec.Marshal(tag))
		return ctx, err == nil
	})

	for {
		select {
		case tag := <-Dag.OnEpoch:
			evt := Event{
				Schema: Epoch,
				Action: "Tag",
				Actor:  *kernel,
				Commit: tag.Root,
				Epoch:  tag.Epoch,
				Depth:  tag.Depth,
			}
			_ = Dispatch(evt, tag)
		}
	}
}

// Commit appends a new event to the dag and stores the payload
func Commit(schema string, action string, actor codec.Oid, payload ...interface{}) (commit codec.Oid, depth uint64, epoch uint64) {
	defer Dag.Unlock()
	Dag.Lock()
	var payloadId codec.Oid
	if len(payload) == 1 {
		data := codec.Marshal(payload[0])
		payloadId = *codec.ToOid(data)
		err := EventStore(payloadId, data) // store payload
		if err != nil {
			Logger.WithFields(map[string]interface{}{"payload": data, "oid": payloadId}).
				Log(logrus.ErrorLevel, "failed to store payload")
		}
	}
	head := Dag.Head()
	event := Event{
		Schema:  schema,
		Action:  action,
		Actor:   actor,
		Payload: payloadId,
		Commit:  head.Root,
		Epoch:   head.Epoch,
		Depth:   head.Depth,
	}
	event.Oid = *codec.ToOid(codec.Marshal(event)) // inject Event.Oid
	err := EventStore(event.Oid, codec.Marshal(event))
	if err != nil {
		Logger.Log(logrus.ErrorLevel, "failed to store payload")
	}

	depth, epoch = Dag.Append(event.Oid, WeldStore)
	err = Dispatch(event, payload...)
	if err != nil {
		Logger.WithFields(map[string]interface{}{"event": event}).
			Log(logrus.ErrorLevel, "commit failed")
	}
	return event.Oid, depth, epoch
}

// Bind appends event handlers for a given schema
func Bind(schema string, handler EventHandler) {
	upstreamHandler, ok := eventMap[schema]
	if !ok {
		eventMap[schema] = handler
	} else {
		eventMap[schema] = func(c context.Context, event Event, entry *logrus.Entry, t *txn.Txn) (ctx context.Context, ok bool) {
			ctx, ok = upstreamHandler(c, event, entry, t)
			if ok {
				return handler(ctx, event, entry, t)
			} else {
				entry.Logf(logrus.ErrorLevel, "Dispatch failed: %s", schema)
			}
			return ctx, ok
		}
	}
}

func WeldStore(id codec.Oid, data []byte) error {
	return Dag.Storage.Set(codec.Cat(db.Welds, id.Bytes()), data)
}

func EventStore(id codec.Oid, data []byte) error {
	return Dag.Storage.Set(id.Bytes(), data)
}
