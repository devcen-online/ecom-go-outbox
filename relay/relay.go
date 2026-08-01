// Package relay реализует публикацию событий outbox в JetStream:
// Nats-Msg-Id = event_id (INV-2), explicit publish, счётчик попыток и переход
// в failed при исчерпании MaxPublishAttempts (INV-9, PRD-033 FR-001/FR-003).
package relay

import (
	"context"
	"errors"
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

// Config — конфигурация relay.
type Config struct {
	// MaxPublishAttempts — порог попыток публикации до статуса failed (INV-9).
	// 0 или отрицательное значение → MaxPublishAttemptsDefault.
	MaxPublishAttempts int
	// BatchLimit — число событий за один проход Pending (0 → все pending).
	BatchLimit int
}

// Relay публикует pending-события outbox в JetStream.
type Relay struct {
	store    outbox.Store
	pub      Publisher
	max      int
	batch    int
	logger   *log.Logger
	onErrLog func(eventID string, err error)
}

// New создаёт relay. onErrLog — опциональный колбэк логирования ошибок
// публикации (по умолчанию — логгер с event_id, без содержимого payload, NFR-004).
func New(store outbox.Store, pub Publisher, cfg Config, l *log.Logger) *Relay {
	max := cfg.MaxPublishAttempts
	if max <= 0 {
		max = MaxPublishAttemptsDefault
	}
	if l == nil {
		l = log.New(log.Writer(), "outbox-relay: ", log.LstdFlags)
	}
	return &Relay{store: store, pub: pub, max: max, batch: cfg.BatchLimit, logger: l}
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

// ErrUnpublished — ошибка, когда ни одно событие не опубликовано.
var ErrUnpublished = errors.New("relay: не опубликовано ни одного события")
