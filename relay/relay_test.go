package relay

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/devcen-online/ecom-go-outbox/outbox"
)

func eventID(i int) string {
	return fmt.Sprintf("11111111-1111-4111-8111-11111111111%d", i)
}

// fakePub — тестовая публикация с управляемым сбоем.
type fakePub struct {
	failCount int // сколько первых публикаций должны падать
	calls     []*Msg
}

func (f *fakePub) Publish(ctx context.Context, msg *Msg) error {
	if f.failCount > 0 {
		f.failCount--
		return errors.New("jetstream: publish timeout")
	}
	f.calls = append(f.calls, msg)
	return nil
}

func setupStore(t *testing.T, count int) *outbox.MemStore {
	t.Helper()
	store := outbox.NewMemStore()
	ctx := context.Background()
	for i := 1; i <= count; i++ {
		tx, err := store.BeginTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ev := &outbox.OutboxEvent{
			ID:               "00000000-0000-4000-8000-000000000000",
			EventID:          eventID(i),
			AggregateID:      "product:p-1",
			AggregateVersion: int64(i),
			Topic:            "ecom.test.catalog.product.updated.v1",
			Payload:          []byte(`{"event_id":"x","event_type":"ecom.catalog.product.updated.v1"}`),
			Status:           outbox.StatusPending,
		}
		if err := store.Append(ctx, tx, ev); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

// FR-003 (подготовка relay): успешная публикация помечает событие published
// и передаёт Nats-Msg-Id = event_id (INV-2).
func TestRunPublishesWithNatsMsgID(t *testing.T) {
	store := setupStore(t, 1)
	pub := &fakePub{}
	r := New(store, pub, Config{}, nil)

	published, err := r.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if published != 1 {
		t.Fatalf("published = %d, want 1", published)
	}
	if len(pub.calls) != 1 {
		t.Fatalf("вызовов Publish = %d, want 1", len(pub.calls))
	}
	if got := pub.calls[0].HeaderValue("Nats-Msg-Id"); got != eventID(1) {
		t.Fatalf("Nats-Msg-Id = %q, want event_id", got)
	}
	ev, err := store.Get(context.Background(), eventID(1))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Status != outbox.StatusPublished || ev.Attempts != 1 {
		t.Fatalf("status = %s attempts = %d, want published/1", ev.Status, ev.Attempts)
	}
}

// INV-9: после успешной публикации повторный Run не трогает published-события.
func TestRunSkipsPublished(t *testing.T) {
	store := setupStore(t, 2)
	pub := &fakePub{}
	r := New(store, pub, Config{}, nil)

	if _, err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pub.calls) != 2 {
		t.Fatalf("вызовов Publish = %d, want 2 (published не перепубликуются)", len(pub.calls))
	}
}

// INV-9: ошибка публикации инкрементирует attempts; до порога — ретрай, на пороге — failed.
func TestRunRetriesThenFailsOnMaxPublishAttempts(t *testing.T) {
	store := setupStore(t, 1)
	pub := &fakePub{failCount: 1} // первая публикация падает, вторая успешна
	r := New(store, pub, Config{MaxPublishAttempts: 3}, nil)

	if published, err := r.Run(context.Background()); err != nil || published != 0 {
		t.Fatalf("Run() = %d, %v; want 0 попыток успешных", published, err)
	}
	ev, _ := store.Get(context.Background(), eventID(1))
	if ev.Status != outbox.StatusPending || ev.Attempts != 1 {
		t.Fatalf("после сбоя status = %s attempts = %d, want pending/1", ev.Status, ev.Attempts)
	}

	if published, err := r.Run(context.Background()); err != nil || published != 1 {
		t.Fatalf("Run() = %d, %v; want 1 опубликовано", published, err)
	}
	ev, _ = store.Get(context.Background(), eventID(1))
	if ev.Status != outbox.StatusPublished || ev.Attempts != 2 {
		t.Fatalf("после ретрая status = %s attempts = %d, want published/2", ev.Status, ev.Attempts)
	}
}

// INV-9 (граница): при attempts == MaxPublishAttempts событие переходит в failed,
// НЕ удаляется и больше не попадает в Pending (нет петли авто-ретраев).
func TestRunExhaustsAttemptsToFailed(t *testing.T) {
	store := setupStore(t, 1)
	pub := &fakePub{failCount: 999} // публикация всегда падает
	r := New(store, pub, Config{MaxPublishAttempts: 3}, nil)

	for i := 0; i < 2; i++ {
		if _, err := r.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	ev, err := store.Get(context.Background(), eventID(1))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Status != outbox.StatusPending || ev.Attempts != 2 {
		t.Fatalf("status = %s attempts = %d, want pending/2 до порога", ev.Status, ev.Attempts)
	}

	if _, err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	ev, err = store.Get(context.Background(), eventID(1))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Status != outbox.StatusFailed {
		t.Fatalf("status = %s, want failed при исчерпании MaxPublishAttempts", ev.Status)
	}
	if ev.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", ev.Attempts)
	}

	pending, _ := store.Pending(context.Background(), 0)
	if len(pending) != 0 {
		t.Fatalf("Pending() = %d событий, want 0 (нет авто-ретраев для failed)", len(pending))
	}
}

// INV-9: MaxDeliver = 1 → первый же сбой сразу отправляет в failed.
func TestRunMaxAttemptsOneFailsImmediately(t *testing.T) {
	store := setupStore(t, 1)
	pub := &fakePub{failCount: 1}
	r := New(store, pub, Config{MaxPublishAttempts: 1}, nil)

	if _, err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	ev, _ := store.Get(context.Background(), eventID(1))
	if ev.Status != outbox.StatusFailed || ev.Attempts != 1 {
		t.Fatalf("status = %s attempts = %d, want failed/1", ev.Status, ev.Attempts)
	}
}

// NFR-004: логи публикации содержат event_id и не содержат содержимого payload.
func TestLogsContainEventIDNotPayload(t *testing.T) {
	var buf strings.Builder
	store := setupStore(t, 1)
	pub := &fakePub{}
	r := New(store, pub, Config{}, testLogger(&buf))

	if _, err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, eventID(1)) {
		t.Fatalf("лог не содержит event_id: %q", out)
	}
	if strings.Contains(out, `"event_id":"x"`) {
		t.Fatalf("лог содержит содержимое payload: %q", out)
	}
}

func testLogger(buf *strings.Builder) *log.Logger {
	return log.New(buf, "", 0)
}
