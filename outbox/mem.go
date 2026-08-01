package outbox

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// MemStore — in-memory реализация Store (test double для unit-тестов и
// смоук-проверок; PostgreSQL-реализация — в отдельном пакете, здесь не нужна).
// Транзакционность имитирует PostgreSQL: BeginTx делает снимок состояния,
// Commit применяет снимок, Rollback его отбрасывает (INV-1).
type MemStore struct {
	mu sync.Mutex

	events  map[string]*OutboxEvent // ключ — event_id
	rows    map[string]string       // «бизнес-таблица» тестового воркспейса (putRow)
	lastIDs map[string]struct{}     // выданные id записей
}

// NewMemStore создаёт пустое in-memory хранилище outbox.
func NewMemStore() *MemStore {
	return &MemStore{
		events:  make(map[string]*OutboxEvent),
		rows:    make(map[string]string),
		lastIDs: make(map[string]struct{}),
	}
}

// memTx — транзакция MemStore.
type memTx struct {
	store  *MemStore
	staged *MemStore
	done   bool
}

func (t *memTx) Commit(ctx context.Context) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.done {
		return errors.New("outbox: транзакция уже завершена")
	}
	t.done = true
	t.store.events = t.staged.events
	t.store.rows = t.staged.rows
	t.store.lastIDs = t.staged.lastIDs
	return nil
}

func (t *memTx) Rollback(ctx context.Context) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.done {
		return errors.New("outbox: транзакция уже завершена")
	}
	t.done = true
	return nil
}

// BeginTx начинает транзакцию со снимком состояния (INV-1).
func (s *MemStore) BeginTx(ctx context.Context) (Tx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stage := NewMemStore()
	for k, v := range s.events {
		stage.events[k] = cloneEvent(v)
	}
	for k, v := range s.rows {
		stage.rows[k] = v
	}
	for k := range s.lastIDs {
		stage.lastIDs[k] = struct{}{}
	}
	return &memTx{store: s, staged: stage}, nil
}

// Append пишет событие в транзакцию tx (INV-1, INV-8 проверяется в AppendRecord).
func (s *MemStore) Append(ctx context.Context, tx Tx, ev *OutboxEvent) error {
	t, ok := tx.(*memTx)
	if !ok {
		return fmt.Errorf("outbox: неверный тип транзакции %T", tx)
	}
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.done {
		return errors.New("outbox: транзакция уже завершена")
	}
	if _, dup := t.staged.events[ev.EventID]; dup {
		return fmt.Errorf("outbox: событие %s уже есть в outbox_events (event_id уникален)", ev.EventID)
	}
	t.staged.events[ev.EventID] = cloneEvent(ev)
	return nil
}

// putRow пишет «бизнес-запись» в ту же транзакцию (test-only: имитация
// бизнес-изменения для проверки INV-1/FR-001 в unit-тестах).
func (s *MemStore) putRow(ctx context.Context, tx Tx, key, value string) error {
	t, ok := tx.(*memTx)
	if !ok {
		return fmt.Errorf("outbox: неверный тип транзакции %T", tx)
	}
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.done {
		return errors.New("outbox: транзакция уже завершена")
	}
	t.staged.rows[key] = value
	return nil
}

// rowValue возвращает «бизнес-запись» по ключу (test-only).
func (s *MemStore) rowValue(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.rows[key]
	return v, ok
}

func (s *MemStore) MarkPublished(ctx context.Context, eventID string, attempts int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev, ok := s.events[eventID]
	if !ok {
		return fmt.Errorf("outbox: событие %s не найдено", eventID)
	}
	ev.Status = StatusPublished
	ev.Attempts = attempts
	return nil
}

func (s *MemStore) MarkFailed(ctx context.Context, eventID string, attempts int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev, ok := s.events[eventID]
	if !ok {
		return fmt.Errorf("outbox: событие %s не найдено", eventID)
	}
	ev.Status = StatusFailed
	ev.Attempts = attempts
	return nil
}

func (s *MemStore) SetAttempts(ctx context.Context, eventID string, attempts int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev, ok := s.events[eventID]
	if !ok {
		return fmt.Errorf("outbox: событие %s не найдено", eventID)
	}
	ev.Attempts = attempts
	return nil
}

func (s *MemStore) Pending(ctx context.Context, limit int) ([]*OutboxEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*OutboxEvent
	for _, ev := range s.events {
		if ev.Status == StatusPending {
			out = append(out, cloneEvent(ev))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *MemStore) Get(ctx context.Context, eventID string) (*OutboxEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev, ok := s.events[eventID]
	if !ok {
		return nil, fmt.Errorf("outbox: событие %s не найдено", eventID)
	}
	return cloneEvent(ev), nil
}

// Count возвращает число записей outbox (test-only).
func (s *MemStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

func cloneEvent(ev *OutboxEvent) *OutboxEvent {
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
