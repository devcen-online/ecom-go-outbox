// Package inbox реализует идемпотентный приём событий: уникальный
// inbox_events.event_id (Nats-Msg-Id) отсекает дубли (FR-002, INV-2, ERD-OB-001).
package inbox

import (
	"context"
	"encoding/json"
	"time"
)

// Status — статус записи inbox (ERD-OB-001: received|processing|processed|failed).
type Status string

const (
	StatusReceived   Status = "received"
	StatusProcessing Status = "processing"
	StatusProcessed  Status = "processed"
	StatusFailed     Status = "failed"
)

// InboxEvent — запись inbox_events (ERD-OB-001, сущность InboxEvent).
type InboxEvent struct {
	ID               string          // uuid записи
	EventID          string          // Nats-Msg-Id, ключ идемпотентности (unique)
	AggregateID      string          // агрегат события
	AggregateVersion int64           // монотонная версия агрегата (>= 1)
	Topic            string          // subject доставки
	Payload          json.RawMessage // payload первого доставленного сообщения (S-5)
	Status           Status
	Attempts         int // число доставок JetStream
	CreatedAt        time.Time
	ProcessedAt      *time.Time
}

// Tx — локальная транзакция хранилища inbox.
type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Store — репозиторий inbox. PostgreSQL-реализация подключается отдельным
// пакетом; in-memory реализация (MemStore) — для unit-тестов.
type Store interface {
	BeginTx(ctx context.Context) (Tx, error)
	// InsertIfAbsent — INSERT ... ON CONFLICT (event_id) DO NOTHING в той же
	// транзакции, что и бизнес-обработка (INV-2). Возвращает inserted=true,
	// если запись создана (событие новое), false — если дубль уже существует.
	InsertIfAbsent(ctx context.Context, tx Tx, ev *InboxEvent) (bool, error)
	// MarkProcessed фиксирует успешную обработку и коммит транзакции.
	MarkProcessed(ctx context.Context, eventID string) error
	// LastAggregateVersion возвращает максимальную применённую версию агрегата
	// (INV-6, BDD-033#S-4). ok=false, если события для агрегата ещё не было.
	LastAggregateVersion(ctx context.Context, aggregateID string) (int64, bool, error)
	Get(ctx context.Context, eventID string) (*InboxEvent, error)
}
