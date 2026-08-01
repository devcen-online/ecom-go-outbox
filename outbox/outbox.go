// Package outbox реализует transactional outbox: запись события в outbox_events
// в одной транзакции с бизнес-изменением (FR-001, ERD-OB-001).
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/devcen-online/ecom-go-outbox/envelope"
)

// Status — статус записи outbox (ERD-OB-001: pending|published|failed).
type Status string

const (
	StatusPending   Status = "pending"
	StatusPublished Status = "published"
	StatusFailed    Status = "failed"
)

// OutboxEvent — запись outbox_events (ERD-OB-001, сущность OutboxEvent).
type OutboxEvent struct {
	ID               string          // uuid записи
	EventID          string          // uuid события = Nats-Msg-Id (INV-2)
	AggregateID      string          // агрегат события
	AggregateVersion int64           // монотонная версия агрегата (>= 1)
	Topic            string          // subject JetStream ecom.<env>.<domain>.<aggregate>.<event>.v1
	Payload          json.RawMessage // полный конверт (envelope) + domain payload
	Status           Status
	Attempts         int // число попыток публикации relay (INV-9)
	CreatedAt        time.Time
	ProcessedAt      *time.Time // момент успешной публикации
}

// Tx — локальная транзакция хранилища outbox (INV-1: запись и бизнес-изменение
// коммитятся/откатываются вместе).
type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Store — репозиторий outbox. PostgreSQL-реализация подключается отдельным
// пакетом; в библиотеке поставляется in-memory реализация (MemStore) для
// unit-тестов и смоук-проверок.
type Store interface {
	BeginTx(ctx context.Context) (Tx, error)
	// Append пишет запись в outbox_events в транзакции tx (INV-1, FR-001).
	Append(ctx context.Context, tx Tx, ev *OutboxEvent) error
	// MarkPublished помечает событие опубликованным после успешной публикации relay.
	MarkPublished(ctx context.Context, eventID string, attempts int) error
	// MarkFailed помечает событие failed при исчерпании MaxPublishAttempts (INV-9).
	MarkFailed(ctx context.Context, eventID string, attempts int) error
	// SetAttempts фиксирует число попыток публикации после сбоя (INV-9,
	// событие остаётся pending до исчерпания MaxPublishAttempts).
	SetAttempts(ctx context.Context, eventID string, attempts int) error
	// Pending возвращает события в статусе pending (для relay), до limit штук.
	Pending(ctx context.Context, limit int) ([]*OutboxEvent, error)
	Get(ctx context.Context, eventID string) (*OutboxEvent, error)
}

// AppendRecord — хелпер библиотеки: валидирует конверт, сверяет колонки с
// полями конверта (INV-8) и вызывает store.Append. Используется сервисами при
// записи бизнес-изменения и события в одной транзакции.
func AppendRecord(ctx context.Context, store Store, tx Tx, ev *OutboxEvent) error {
	if ev == nil {
		return errors.New("outbox: событие не может быть nil")
	}
	env, err := envelope.Parse(ev.Payload)
	if err != nil {
		return fmt.Errorf("outbox: невалидный конверт в payload: %w", err)
	}
	if err := checkColumnsMatchEnvelope(ev, env); err != nil {
		return err
	}
	if ev.ID == "" {
		return errors.New("outbox: id записи не может быть пустым")
	}
	if ev.EventID == "" || ev.AggregateID == "" || ev.Topic == "" {
		return errors.New("outbox: event_id, aggregate_id и topic обязательны")
	}
	if ev.Status == "" {
		ev.Status = StatusPending
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now().UTC()
	}
	return store.Append(ctx, tx, ev)
}

// checkColumnsMatchEnvelope реализует INV-8 (MIN-4 review-33-artifacts):
// колонки таблицы должны совпадать с соответствующими полями конверта.
func checkColumnsMatchEnvelope(ev *OutboxEvent, env *envelope.Envelope) error {
	var errs []string
	if ev.EventID != env.EventID {
		errs = append(errs, fmt.Sprintf("event_id колонки %q != event_id конверта %q", ev.EventID, env.EventID))
	}
	if ev.AggregateID != env.AggregateID {
		errs = append(errs, fmt.Sprintf("aggregate_id колонки %q != aggregate_id конверта %q", ev.AggregateID, env.AggregateID))
	}
	if ev.AggregateVersion != env.AggregateVersion {
		errs = append(errs, fmt.Sprintf("aggregate_version колонки %d != aggregate_version конверта %d", ev.AggregateVersion, env.AggregateVersion))
	}
	// topic (subject с env) должен оканчиваться на event_type без префикса "ecom." (EVENTS-001 §1).
	suffix := strings.TrimPrefix(env.EventType, "ecom.")
	if ev.Topic == suffix || !strings.HasSuffix(ev.Topic, "."+suffix) {
		errs = append(errs, fmt.Sprintf("topic %q не соответствует event_type %q (нужен subject ecom.<env>.%s)", ev.Topic, env.EventType, suffix))
	}
	if len(errs) > 0 {
		return fmt.Errorf("outbox: INV-8 нарушен: %s", strings.Join(errs, "; "))
	}
	return nil
}
