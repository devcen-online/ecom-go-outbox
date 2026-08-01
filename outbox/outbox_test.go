package outbox

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

const (
	testEventID = "11111111-1111-4111-8111-111111111111"
	testTopic   = "ecom.test.catalog.product.created.v1"
	testType    = "ecom.catalog.product.created.v1"
)

func newTestEvent() *OutboxEvent {
	return &OutboxEvent{
		ID:               "9f0f0000-0000-4000-8000-000000000001",
		EventID:          testEventID,
		AggregateID:      "product:p-1",
		AggregateVersion: 1,
		Topic:            testTopic,
		Payload:          envelopePayload(testEventID, "product:p-1", 1),
		Status:           StatusPending,
		Attempts:         0,
		CreatedAt:        time.Now().UTC(),
	}
}

// envelopePayload строит конверт, согласованный с колонками (INV-8).
func envelopePayload(eventID, aggregateID string, version int64) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{
		"event_id":          eventID,
		"event_type":        testType,
		"schema_version":    1,
		"aggregate_id":      aggregateID,
		"aggregate_version": version,
		"occurred_at":       time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC),
		"producer":          "catalog-service",
		"payload":           map[string]any{"name": "p-1"},
	})
	return raw
}

// S-1 (FR-001): COMMIT → существуют и бизнес-запись, и запись outbox.
func TestCommitPersistsBusinessAndOutbox(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	tx, err := store.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.putRow(ctx, tx, "products:p-1", "created"); err != nil {
		t.Fatal(err)
	}
	if err := AppendRecord(ctx, store, tx, newTestEvent()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if v, ok := store.rowValue("products:p-1"); !ok || v != "created" {
		t.Fatalf("бизнес-запись отсутствует после COMMIT (ok=%v v=%q)", ok, v)
	}
	ev, err := store.Get(ctx, testEventID)
	if err != nil || ev.EventID != testEventID {
		t.Fatalf("запись outbox отсутствует после COMMIT: err=%v", err)
	}
	if store.Count() != 1 {
		t.Fatalf("Count() = %d, want 1", store.Count())
	}
}

// S-2 (FR-001, negative): ROLLBACK → ни бизнес-записи, ни записи outbox.
func TestRollbackRemovesBusinessAndOutbox(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	tx, err := store.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.putRow(ctx, tx, "products:p-2", "created"); err != nil {
		t.Fatal(err)
	}
	ev := newTestEvent()
	ev.EventID = "22222222-2222-4222-8222-222222222222"
	ev.Payload = envelopePayload(ev.EventID, ev.AggregateID, ev.AggregateVersion)
	if err := AppendRecord(ctx, store, tx, ev); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	if _, ok := store.rowValue("products:p-2"); ok {
		t.Fatal("бизнес-запись существует после ROLLBACK")
	}
	if store.Count() != 0 {
		t.Fatalf("записей outbox %d после ROLLBACK, want 0", store.Count())
	}
}

// INV-8: расхождение колонок и полей конверта → ошибка записи.
func TestAppendRejectsColumnEnvelopeMismatch(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	tx, _ := store.BeginTx(ctx)

	cases := []struct {
		name string
		mut  func(ev *OutboxEvent)
	}{
		{"event_id", func(ev *OutboxEvent) { ev.EventID = "99999999-9999-4999-8999-999999999999" }},
		{"aggregate_id", func(ev *OutboxEvent) { ev.AggregateID = "product:other" }},
		{"aggregate_version", func(ev *OutboxEvent) { ev.AggregateVersion = 5 }},
		{"topic", func(ev *OutboxEvent) { ev.Topic = "ecom.test.other.topic.v1" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := newTestEvent()
			tc.mut(ev)
			err := AppendRecord(ctx, store, tx, ev)
			if err == nil {
				t.Fatal("AppendRecord() = nil, want INV-8 error")
			}
		})
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if store.Count() != 0 {
		t.Fatalf("после Rollback записей %d, want 0", store.Count())
	}
}

// INV-8: согласованные колонки и конверт → запись проходит.
func TestAppendAcceptsMatchingEnvelope(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	tx, _ := store.BeginTx(ctx)
	if err := AppendRecord(ctx, store, tx, newTestEvent()); err != nil {
		t.Fatalf("AppendRecord() error = %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if store.Count() != 1 {
		t.Fatalf("Count() = %d, want 1", store.Count())
	}
}

// INV-9: Pending возвращает только pending-события; published/failed исключаются.
func TestPendingOnlyPending(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	for _, id := range []string{
		"11111111-1111-4111-8111-111111111111",
		"22222222-2222-4222-8222-222222222222",
		"33333333-3333-4333-8333-333333333333",
	} {
		tx, _ := store.BeginTx(ctx)
		ev := newTestEvent()
		ev.EventID = id
		ev.Payload = envelopePayload(ev.EventID, ev.AggregateID, ev.AggregateVersion)
		if err := AppendRecord(ctx, store, tx, ev); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.MarkPublished(ctx, "22222222-2222-4222-8222-222222222222", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkFailed(ctx, "33333333-3333-4333-8333-333333333333", 10); err != nil {
		t.Fatal(err)
	}

	pending, err := store.Pending(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].EventID != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("Pending() = %+v, want только первое событие", pending)
	}
}
