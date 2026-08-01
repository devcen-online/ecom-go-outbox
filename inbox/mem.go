package inbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// MemStore — in-memory реализация Store (test double). InsertIfAbsent повторяет
// семантику INSERT ... ON CONFLICT (event_id) DO NOTHING (INV-2): при дубле
// возвращает false и НЕ меняет существующую запись (S-5: payload первого доставления).
type MemStore struct {
	mu     sync.Mutex
	events map[string]*InboxEvent // ключ — event_id (unique constraint)
	// CommitHook — test-only: вызывается после успешного Commit (проверка
	// порядка commit → ack, BDD-033#S-10 / FR-006).
	CommitHook func()
}

// NewMemStore создаёт пустое in-memory хранилище inbox.
func NewMemStore() *MemStore {
	return &MemStore{events: make(map[string]*InboxEvent)}
}

// memTx — транзакция MemStore (snapshot на BeginTx, как в outbox.MemStore).
type memTx struct {
	store  *MemStore
	staged *MemStore
	done   bool
}

func (t *memTx) Commit(ctx context.Context) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.done {
		return errors.New("inbox: транзакция уже завершена")
	}
	t.done = true
	t.store.events = t.staged.events
	if t.store.CommitHook != nil {
		t.store.CommitHook()
	}
	return nil
}

func (t *memTx) Rollback(ctx context.Context) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.done {
		return errors.New("inbox: транзакция уже завершена")
	}
	t.done = true
	return nil
}

func (s *MemStore) BeginTx(ctx context.Context) (Tx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stage := NewMemStore()
	for k, v := range s.events {
		stage.events[k] = cloneEvent(v)
	}
	return &memTx{store: s, staged: stage}, nil
}

func (s *MemStore) InsertIfAbsent(ctx context.Context, tx Tx, ev *InboxEvent) (bool, error) {
	t, ok := tx.(*memTx)
	if !ok {
		return false, fmt.Errorf("inbox: неверный тип транзакции %T", tx)
	}
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.done {
		return false, errors.New("inbox: транзакция уже завершена")
	}
	if _, exists := t.staged.events[ev.EventID]; exists {
		return false, nil // ON CONFLICT DO NOTHING: дубль, запись не меняется
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now().UTC()
	}
	t.staged.events[ev.EventID] = cloneEvent(ev)
	return true, nil
}

func (s *MemStore) MarkProcessed(ctx context.Context, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev, ok := s.events[eventID]
	if !ok {
		return fmt.Errorf("inbox: событие %s не найдено", eventID)
	}
	ev.Status = StatusProcessed
	t := time.Now().UTC()
	ev.ProcessedAt = &t
	return nil
}

func (s *MemStore) LastAggregateVersion(ctx context.Context, aggregateID string) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var max int64
	found := false
	for _, ev := range s.events {
		if ev.AggregateID == aggregateID && ev.AggregateVersion > max {
			max = ev.AggregateVersion
			found = true
		}
	}
	return max, found, nil
}

func (s *MemStore) Get(ctx context.Context, eventID string) (*InboxEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev, ok := s.events[eventID]
	if !ok {
		return nil, fmt.Errorf("inbox: событие %s не найдено", eventID)
	}
	return cloneEvent(ev), nil
}

// Count возвращает число записей inbox (test-only).
func (s *MemStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

func cloneEvent(ev *InboxEvent) *InboxEvent {
	c := *ev
	if ev.Payload != nil {
		c.Payload = append([]byte(nil), ev.Payload...)
	}
	if ev.ProcessedAt != nil {
		t := *ev.ProcessedAt
		c.ProcessedAt = &t
	}
	return &c
}
