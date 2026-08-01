// Package consumer реализует идемпотентного консьюмера JetStream-событий
// (inbox): BEGIN → idempotency-insert → обработчик → COMMIT → ACK (ADR-001,
// INV-3). При ошибке обработки — ROLLBACK → NACK (redelivery, FR-004).
// Сбой между COMMIT и ACK безопасен: redelivery отсекается уникальным
// inbox_events.event_id (FR-007, INV-7).
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/devcen-online/ecom-go-outbox/envelope"
	"github.com/devcen-online/ecom-go-outbox/inbox"
)

// ErrMissingMsgID — сообщение без заголовка Nats-Msg-Id (контракт нарушен).
var ErrMissingMsgID = errors.New("consumer: отсутствует заголовок Nats-Msg-Id")

// Logger — минимальный логгер (stdlib *log.Logger удовлетворяет интерфейсу).
type Logger interface {
	Printf(format string, args ...any)
}

// Message — сообщение JetStream (адаптер для nats.go — пакет js).
type Message interface {
	Subject() string
	Data() []byte
	HeaderValue(name string) string
	// DeliveryCount — число доставок (JetStream Metadata.NumDelivered /
	// заголовок Nats-Deliveries). Используется для исчерпания MaxDeliver (INV-4).
	DeliveryCount() uint64
	Ack() error
	Nack() error
	// Term — терминация: сообщение не передоставляется (переход в dead-letter,
	// петли нет, FR-005/S-12).
	Term() error
}

// Handler — бизнес-обработчик доменного payload (вызывается ровно один раз на event_id).
type Handler func(ctx context.Context, payload []byte) error

// DeadLetterMessage — событие, исчерпавшее MaxDeliver, для dead-letter потока
// (INV-5, FR-005): исходный payload + метаданные (причина, число попыток).
type DeadLetterMessage struct {
	// Subject — subject dead-letter потока (по умолчанию исходный + ".dl").
	Subject string
	// OriginalSubject — subject, с которого пришло сообщение.
	OriginalSubject string
	// EventID — Nats-Msg-Id исходного сообщения.
	EventID string
	// Reason — причина перехода в dead-letter (INV-5).
	Reason string
	// Attempts — число доставок до исчерпания (INV-5).
	Attempts uint64
	// Payload — исходный payload сообщения (INV-5).
	Payload []byte
}

// DeadLetterSink публикует сообщение в dead-letter поток (адаптер JetStream — пакет js).
type DeadLetterSink interface {
	PublishDeadLetter(ctx context.Context, dl *DeadLetterMessage) error
}

// Config — конфигурация consumer.
type Config struct {
	Logger Logger
	// MaxDeliver — максимальное число доставок; при исчерпании (ошибка обработки)
	// сообщение уходит в dead-letter через DeadLetterSink и терминуется (INV-4,
	// FR-004, FR-005). 0 — без ограничения (бесконечные ретраи не рекомендуются).
	MaxDeliver uint64
	// DeadLetterSink — публикация в dead-letter поток (INV-5). Обязателен при
	// MaxDeliver > 0.
	DeadLetterSink DeadLetterSink
	// AfterCommit — test-only hook (fault injection «crash after commit, before ack»,
	// PRD-033 Q-2, BDD-033#S-11): вызывается после COMMIT, до ACK.
	AfterCommit func()
}

// Consumer обрабатывает сообщения JetStream через inbox (идемпотентность).
type Consumer struct {
	inbox      inbox.Store
	handler    Handler
	logger     Logger
	maxDeliver uint64
	dlSink     DeadLetterSink
	after      func()
}

// New создаёт consumer.
func New(is inbox.Store, h Handler, cfg Config) *Consumer {
	l := cfg.Logger
	if l == nil {
		l = log.New(log.Writer(), "inbox-consumer: ", log.LstdFlags)
	}
	return &Consumer{inbox: is, handler: h, logger: l, maxDeliver: cfg.MaxDeliver, dlSink: cfg.DeadLetterSink, after: cfg.AfterCommit}
}

// Handle обрабатывает одно сообщение по протоколу ADR-001:
//
//	BEGIN → InsertIfAbsent → (дубль: лог «duplicate ignored», ACK, выход)
//	→ обработчик → COMMIT → [AfterCommit] → ACK
//	ошибка обработчика → ROLLBACK → NACK
//
// В порядке операций commit всегда раньше ack (FR-006, BDD-033#S-10).
func (c *Consumer) Handle(ctx context.Context, m Message) error {
	eventID := m.HeaderValue("Nats-Msg-Id")
	if eventID == "" {
		c.logger.Printf("missing Nats-Msg-Id subject=%s", m.Subject())
		return fmt.Errorf("%w subject=%s", ErrMissingMsgID, m.Subject())
	}

	env, err := envelope.Parse(m.Data())
	if err != nil {
		// Сообщение без валидного конверта: nack вернёт то же самое —
		// подтверждаем и фиксируем в логе, исключая молчаливую потерю (FR-005).
		c.logger.Printf("invalid envelope event_id=%s: %v", eventID, err)
		return m.Ack()
	}

	tx, err := c.inbox.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("consumer: begin tx: %w", err)
	}

	record := &inbox.InboxEvent{
		EventID:          env.EventID,
		AggregateID:      env.AggregateID,
		AggregateVersion: env.AggregateVersion,
		Topic:            m.Subject(),
		Payload:          m.Data(),
		Status:           inbox.StatusReceived,
		Attempts:         0,
		CreatedAt:        time.Now().UTC(),
	}
	inserted, err := c.inbox.InsertIfAbsent(ctx, tx, record)
	if err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("consumer: insert inbox: %w", err)
	}
	if !inserted {
		// INV-2 (FR-002, S-3, S-5): дубль Nats-Msg-Id — обработчик не вызывается,
		// лог «duplicate ignored» с id (NFR-004). Запись (payload первого
		// доставления) не меняется.
		_ = tx.Rollback(ctx)
		c.logger.Printf("duplicate ignored event_id=%s", eventID)
		return m.Ack()
	}

	if err := c.handler(ctx, m.Data()); err != nil {
		_ = tx.Rollback(ctx)
		c.logger.Printf("handler failed event_id=%s: %v", eventID, err)
		// INV-4 (FR-004): при исчерпании MaxDeliver — dead-letter, а не молчаливая
		// потеря и не бесконечный ретрай; терминируем (петли нет, FR-005/S-12).
		if c.maxDeliver > 0 && m.DeliveryCount() >= c.maxDeliver {
			return c.moveToDeadLetter(ctx, m, eventID, err)
		}
		return m.Nack() // redelivery по AckWait (FR-004)
	}

	if err := tx.Commit(ctx); err != nil {
		c.logger.Printf("commit failed event_id=%s: %v", eventID, err)
		return m.Nack()
	}
	// COMMIT выполнен — далее ACK (INV-3: порядок commit → ack).
	if c.after != nil {
		c.after() // fault injection S-11: «crash after commit, before ack»
	}
	return m.Ack()
}

// moveToDeadLetter публикует сообщение в dead-letter поток (INV-5: исходный
// payload + причина + число попыток) и терминует его в основном потоке.
func (c *Consumer) moveToDeadLetter(ctx context.Context, m Message, eventID string, cause error) error {
	if c.dlSink == nil {
		c.logger.Printf("dead-letter sink не настроен event_id=%s — терминация без публикации", eventID)
		return m.Term()
	}
	dl := &DeadLetterMessage{
		Subject:         m.Subject() + ".dl",
		OriginalSubject: m.Subject(),
		EventID:         eventID,
		Reason:          cause.Error(),
		Attempts:        m.DeliveryCount(),
		Payload:         m.Data(),
	}
	if err := c.dlSink.PublishDeadLetter(ctx, dl); err != nil {
		c.logger.Printf("dead-letter publish failed event_id=%s: %v", eventID, err)
		return m.Term() // даже при сбое публикации — без петли redelivery
	}
	c.logger.Printf("moved to dead-letter event_id=%s attempts=%d subject=%s", eventID, dl.Attempts, dl.Subject)
	return m.Term()
}
