// Package js содержит адаптер JetStream для relay/consumer (nats.go) и
// интеграционные тесты за build-тегом integration (FR-003..FR-007).
//
// Интеграционные тесты не входят в go test ./... и требуют доступного
// NATS-сервера с JetStream: go test -tags integration ./js/ -run Integration.
package js

import (
	"context"
	"errors"
	"fmt"

	"github.com/devcen-online/ecom-go-outbox/relay"
	"github.com/nats-io/nats.go/jetstream"
)

// Publisher адаптирует jetstream.JetStream к relay.Publisher
// (Nats-Msg-Id = event_id передаётся заголовком, INV-2).
type Publisher struct {
	js jetstream.JetStream
}

// NewPublisher создаёт адаптер публикации поверх JetStream.
func NewPublisher(js jetstream.JetStream) *Publisher {
	return &Publisher{js: js}
}

// Publish публикует сообщение в JetStream (at-least-once: Nats-Msg-Id
// включает дедупликацию native JetStream для повторов relay).
func (p *Publisher) Publish(ctx context.Context, msg *relay.Msg) error {
	if msg == nil || msg.Subject == "" {
		return errors.New("js: сообщение с пустым subject")
	}
	if _, err := p.js.PublishMsg(ctx, natsMsg(msg)); err != nil {
		return fmt.Errorf("js: publish %s: %w", msg.Subject, err)
	}
	return nil
}
