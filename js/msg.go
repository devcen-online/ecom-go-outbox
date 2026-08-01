package js

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/devcen-online/ecom-go-outbox/consumer"
	"github.com/devcen-online/ecom-go-outbox/relay"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// natsMsg преобразует relay.Msg в nats.Msg для публикации в JetStream.
func natsMsg(msg *relay.Msg) *nats.Msg {
	m := &nats.Msg{Subject: msg.Subject, Data: msg.Payload, Header: nats.Header{}}
	for k, vs := range msg.Header {
		for _, v := range vs {
			m.Header.Add(k, v)
		}
	}
	return m
}

// Message адаптирует jetstream.Msg к consumer.Message (Subject/Data/Ack/Term —
// из встроенного jetstream.Msg, HeaderValue/DeliveryCount/Nack — свои).
type Message struct {
	jetstream.Msg
	// AckWait — окно redelivery для Nack (BDD-033#S-7, NFR-003): при 0 —
	// немедленный redelivery (семантика Nak в JetStream).
	AckWait time.Duration
}

// HeaderValue возвращает первое значение заголовка сообщения JetStream.
func (m *Message) HeaderValue(name string) string {
	if v := m.Msg.Headers().Get(name); v != "" {
		return v
	}
	return ""
}

// DeliveryCount — число доставок сообщения (JetStream Metadata.NumDelivered).
func (m *Message) DeliveryCount() uint64 {
	md, err := m.Msg.Metadata()
	if err != nil || md == nil {
		return 0
	}
	return md.NumDelivered
}

// Nack — негативное подтверждение: redelivery с задержкой AckWait (S-7) либо
// немедленный (семантика Nak в JetStream).
func (m *Message) Nack() error {
	if m.AckWait > 0 {
		return m.Msg.NakWithDelay(m.AckWait)
	}
	return m.Msg.Nak()
}

var _ consumer.Message = (*Message)(nil)

// DeadLetterSink адаптирует jetstream.JetStream к consumer.DeadLetterSink:
// публикует в subject dead-letter с метаданными в заголовках (INV-5: причина
// отказа, число попыток; FR-005).
type DeadLetterSink struct {
	js jetstream.JetStream
}

// NewDeadLetterSink создаёт адаптер dead-letter публикации.
func NewDeadLetterSink(js jetstream.JetStream) *DeadLetterSink {
	return &DeadLetterSink{js: js}
}

// PublishDeadLetter публикует сообщение в dead-letter поток.
func (s *DeadLetterSink) PublishDeadLetter(ctx context.Context, dl *consumer.DeadLetterMessage) error {
	if dl == nil || dl.Subject == "" {
		return errors.New("js: dead-letter сообщение с пустым subject")
	}
	msg := &nats.Msg{
		Subject: dl.Subject,
		Data:    dl.Payload,
		Header: nats.Header{
			"Nats-Msg-Id":      []string{dl.EventID},
			"Nats-Dead-Letter": []string{dl.OriginalSubject},
			"Nats-Reason":      []string{dl.Reason},
			"Nats-Attempts":    []string{fmt.Sprintf("%d", dl.Attempts)},
		},
	}
	if _, err := s.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("js: publish dead-letter %s: %w", dl.Subject, err)
	}
	return nil
}

var _ consumer.DeadLetterSink = (*DeadLetterSink)(nil)
