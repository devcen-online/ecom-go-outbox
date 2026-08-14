// Package relay реализует публикацию событий outbox в JetStream:
// Nats-Msg-Id = event_id (INV-2), explicit publish, счётчик попыток и переход
// в failed при исчерпании MaxPublishAttempts (INV-9, PRD-033 FR-001/FR-003).
package relay

import (
	"context"
	"fmt"
	"log"

	"github.com/devcen-online/ecom-go-outbox/outbox"
)

// MaxPublishAttemptsDefault — конфигурируемый порог попыток публикации (INV-9, дефолт 10).
const MaxPublishAttemptsDefault = 10

// Msg — сообщение для публикации в JetStream (без зависимостей от nats.go).
type Msg struct {
	Subject string              // subject публикации ecom.<env>.<domain>.<aggregate>.<event>.v1
	Header  map[string][]string // Nats-Msg-Id = event_id, и др.
	Payload []byte              // полный конверт
}

// HeaderValue возвращает первое значение заголовка.
func (m *Msg) HeaderValue(name string) string {
	if v := m.Header[name]; len(v) > 0 {
		return v[0]
	}
	return ""
}

// Publisher — публикация в JetStream (адаптер для nats.go — пакет js).
type Publisher interface {
	Publish(ctx context.Context, msg *Msg) error
}

// DeadLetterMessage — событие, исчерпавшее MaxPublishAttempts, для dead-letter потока
// (short-plan.md §1): исходный payload + метаданные (причина, число попыток).
type DeadLetterMessage struct {
	// Subject — subject dead-letter потока (исходный топик + ".dl").
	Subject string
	// EventID — event_id события outbox.
	EventID string
	// Reason — причина перехода в dead-letter.
	Reason string
	// Attempts — число попыток публикации до исчерпания.
	Attempts int
	// Payload — исходный payload события.
	Payload []byte
}

// DeadLetterSink публикует событие в dead-letter поток (адаптер JetStream — пакет js).
type DeadLetterSink interface {
	PublishDeadLetter(ctx context.Context, dl *DeadLetterMessage) error
}

// Config — конфигурация relay.
type Config struct {
	// MaxPublishAttempts — порог попыток публикации до статуса failed (INV-9).
	// 0 или отрицательное значение → MaxPublishAttemptsDefault.
	MaxPublishAttempts int
	// BatchLimit — число событий за один проход Pending (0 → все pending).
	BatchLimit int
	// DeadLetterSink — публикация в dead-letter поток при исчерпании MaxPublishAttempts.
	// Необязателен: если не задан, событие только помечается failed без публикации.
	DeadLetterSink DeadLetterSink
}

// Relay публикует pending-события outbox в JetStream.
type Relay struct {
	store  outbox.Store
	pub    Publisher
	max    int
	batch  int
	logger *log.Logger
	dlSink DeadLetterSink
}

// New создаёт relay.
func New(store outbox.Store, pub Publisher, cfg Config, l *log.Logger) *Relay {
	max := cfg.MaxPublishAttempts
	if max <= 0 {
		max = MaxPublishAttemptsDefault
	}
	if l == nil {
		l = log.New(log.Writer(), "outbox-relay: ", log.LstdFlags)
	}
	return &Relay{store: store, pub: pub, max: max, batch: cfg.BatchLimit, logger: l, dlSink: cfg.DeadLetterSink}
}

// Run выполняет один проход: читает pending-события, публикует каждое,
// при успехе — published, при ошибке — attempts++ и failed при attempts >= max (INV-9).
// Возвращает число успешно опубликованных событий.
func (r *Relay) Run(ctx context.Context) (int, error) {
	events, err := r.store.Pending(ctx, r.batch)
	if err != nil {
		return 0, fmt.Errorf("relay: чтение pending: %w", err)
	}
	published := 0
	for _, ev := range events {
		if ev.Attempts >= r.max {
			// INV-9: попытки исчерпаны ранее — статус failed, авто-ретраев нет.
			if err := r.store.MarkFailed(ctx, ev.EventID, ev.Attempts); err != nil {
				return published, fmt.Errorf("relay: mark failed %s: %w", ev.EventID, err)
			}
			r.logger.Printf("publish attempts exhausted event_id=%s attempts=%d max=%d", ev.EventID, ev.Attempts, r.max)
			r.publishDeadLetter(ctx, ev.EventID, ev.Attempts, ev.Payload, ev.Topic)
			continue
		}
		msg := &Msg{
			Subject: ev.Topic,
			Header:  map[string][]string{"Nats-Msg-Id": {ev.EventID}},
			Payload: ev.Payload,
		}
		if err := r.pub.Publish(ctx, msg); err != nil {
			attempts := ev.Attempts + 1
			if attempts >= r.max {
				if err := r.store.MarkFailed(ctx, ev.EventID, attempts); err != nil {
					return published, fmt.Errorf("relay: mark failed %s: %w", ev.EventID, err)
				}
				r.logger.Printf("publish failed attempts exhausted event_id=%s attempts=%d max=%d", ev.EventID, attempts, r.max)
				r.publishDeadLetter(ctx, ev.EventID, attempts, ev.Payload, ev.Topic)
			} else {
				// INV-9: попытка не удалась — фиксируем счётчик, событие остаётся
				// pending для следующего прохода (ретрай до MaxPublishAttempts).
				if err := r.store.SetAttempts(ctx, ev.EventID, attempts); err != nil {
					return published, fmt.Errorf("relay: set attempts %s: %w", ev.EventID, err)
				}
				r.logger.Printf("publish failed, will retry event_id=%s attempts=%d", ev.EventID, attempts)
			}
			continue
		}
		if err := r.store.MarkPublished(ctx, ev.EventID, ev.Attempts+1); err != nil {
			return published, fmt.Errorf("relay: mark published %s: %w", ev.EventID, err)
		}
		r.logger.Printf("published event_id=%s subject=%s", ev.EventID, ev.Topic)
		published++
	}
	return published, nil
}

// publishDeadLetter отправляет событие в dead-letter поток, если sink настроен.
// Ошибка публикации только логируется — событие уже помечено failed.
func (r *Relay) publishDeadLetter(ctx context.Context, eventID string, attempts int, payload []byte, topic string) {
	if r.dlSink == nil {
		return
	}
	dl := &DeadLetterMessage{
		Subject:  topic + ".dl",
		EventID:  eventID,
		Reason:   "publish attempts exhausted",
		Attempts: attempts,
		Payload:  payload,
	}
	if err := r.dlSink.PublishDeadLetter(ctx, dl); err != nil {
		r.logger.Printf("dead-letter publish failed event_id=%s: %v", eventID, err)
	} else {
		r.logger.Printf("moved to dead-letter event_id=%s attempts=%d subject=%s", eventID, attempts, dl.Subject)
	}
}
