//go:build integration

// Интеграционные тесты JetStream (PRD-033 FR-003..FR-007, BDD-033#S-6..S-12).
// Требуют доступного NATS-сервера с JetStream:
//
//	export NATS_URL=nats://127.0.0.1:4222
//	go test -tags integration ./js/
//
// Конфигурация (AckWait, MaxDeliver, retention) идентична боевой docker-compose
// (NFR-003, R-2). В go test ./... не входят.
package js_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/devcen-online/ecom-go-outbox/consumer"
	"github.com/devcen-online/ecom-go-outbox/envelope"
	"github.com/devcen-online/ecom-go-outbox/inbox"
	"github.com/devcen-online/ecom-go-outbox/js"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	subject   = "ecom.test.catalog.product.updated.v1"
	dlSubject = "ecom.test.catalog.product.updated.v1.dl"
)

type jsEnv struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	cons     jetstream.Consumer
	dlStream string
}

func connect(t *testing.T) *jsEnv {
	t.Helper()
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Skipf("NATS недоступен (%v) — интеграционный тест пропущен", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	// Уникальные имена стримов на прогон, чтобы тесты не задевали друг друга.
	run := fmt.Sprintf("%d", time.Now().UnixNano())
	stream := "ECOM_" + run
	dlStream := "ECOM_DLC_" + run

	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:              stream,
		Subjects:          []string{subject},
		Retention:         jetstream.LimitsPolicy,
		MaxAge:            5 * time.Minute,
		Duplicates:        5 * time.Minute,
		Discard:           jetstream.DiscardNew,
		MaxMsgsPerSubject: 10000,
		Storage:           jetstream.FileStorage,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(ctx, stream) })

	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      dlStream,
		Subjects:  []string{dlSubject},
		Storage:   jetstream.FileStorage,
		MaxAge:    5 * time.Minute,
		Retention: jetstream.LimitsPolicy,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(ctx, dlStream) })

	cons, err := js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:       "test-consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       2 * time.Second,
		MaxDeliver:    3,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = js.DeleteConsumer(ctx, stream, "test-consumer") })

	return &jsEnv{nc: nc, js: js, cons: cons, dlStream: dlStream}
}

func (e *jsEnv) publish(t *testing.T, eventID string, ver int64) []byte {
	t.Helper()
	raw := testEnvelope(t, eventID, ver)
	if _, err := e.js.PublishMsg(context.Background(), &nats.Msg{
		Subject: subject,
		Data:    raw,
		Header:  nats.Header{"Nats-Msg-Id": []string{eventID}},
	}); err != nil {
		t.Fatal(err)
	}
	return raw
}

func testEnvelope(t *testing.T, eventID string, ver int64) []byte {
	t.Helper()
	e := envelope.Envelope{
		EventID:          eventID,
		EventType:        "ecom.catalog.product.updated.v1",
		SchemaVersion:    1,
		AggregateID:      "product:p-1",
		AggregateVersion: ver,
		OccurredAt:       time.Now().UTC(),
		Producer:         "integration-test",
		Payload:          []byte(fmt.Sprintf(`{"price":%d}`, ver)),
	}
	raw, err := e.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// S-6 (FR-003, smoke): round-trip publish → consume 1000 сообщений без потерь
// (NFR-001: published == consumed).
func TestIntegrationRoundTrip1000(t *testing.T) {
	e := connect(t)
	const n = 1000

	for i := 0; i < n; i++ {
		e.publish(t, fmt.Sprintf("%08d-0000-4000-8000-000000000000", i), int64(i+1))
	}

	var mu sync.Mutex
	received, processed := 0, 0
	is := inbox.NewMemStore()
	c := consumer.New(is, func(ctx context.Context, payload []byte) error {
		mu.Lock()
		processed++
		mu.Unlock()
		return nil
	}, consumer.Config{})

	cc, err := e.cons.Consume(func(msg jetstream.Msg) {
		mu.Lock()
		received++
		mu.Unlock()
		if err := c.Handle(context.Background(), &js.Message{Msg: msg}); err != nil {
			t.Errorf("Handle: %v", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Stop()

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := received == n && processed == n
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if received != n {
		t.Fatalf("consumed = %d, want %d (потерь быть не должно, NFR-001)", received, n)
	}
	if processed != n {
		t.Fatalf("processed = %d, want %d", processed, n)
	}
}

// S-7 (FR-004, smoke): nack → redelivery в окне AckWait (при AckWait=2s — не
// раньше 1s и не позже 3s, NFR-003); счётчик доставок увеличивается.
func TestIntegrationNackRedelivery(t *testing.T) {
	e := connect(t)
	e.publish(t, "99999999-0000-4000-8000-000000000001", 1)

	deliveries := make(chan uint64, 8)
	cc, err := e.cons.Consume(func(msg jetstream.Msg) {
		wrapped := &js.Message{Msg: msg, AckWait: 2 * time.Second}
		if md, err := msg.Metadata(); err == nil {
			deliveries <- md.NumDelivered
		}
		_ = wrapped.Nack()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Stop()

	waitDelivery(t, deliveries, 1, "первая доставка")
	t1 := time.Now()
	waitDelivery(t, deliveries, 2, "redelivery после nack")
	delay := time.Since(t1)

	if delay < 1*time.Second || delay > 3*time.Second {
		t.Fatalf("redelivery через %v, want в окне [1s, 3s] при AckWait=2s (NFR-003)", delay)
	}
}

func waitDelivery(t *testing.T, ch <-chan uint64, want uint64, what string) {
	t.Helper()
	select {
	case d := <-ch:
		if d != want {
			t.Fatalf("%s: NumDelivered = %d, want %d", what, d, want)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("timeout: %s (NumDelivered %d) не получена", what, want)
	}
}

// S-8 + S-9 (FR-004, FR-005): исчерпание MaxDeliver (3 ошибки обработки) →
// dead-letter с исходным payload и метаданными (причина, число попыток 3),
// без возврата в основной поток (S-12).
func TestIntegrationMaxDeliverDeadLetter(t *testing.T) {
	e := connect(t)
	evID := "88888888-0000-4000-8000-000000000002"
	raw := e.publish(t, evID, 1)

	// Консьюмер библиотеки с MaxDeliver=3: обработчик всегда падает.
	is := inbox.NewMemStore()
	dlSink := js.NewDeadLetterSink(e.js)
	c := consumer.New(is, func(ctx context.Context, payload []byte) error {
		return context.DeadlineExceeded
	}, consumer.Config{MaxDeliver: 3, DeadLetterSink: dlSink})

	done := make(chan error, 3)
	cc, err := e.cons.Consume(func(msg jetstream.Msg) {
		done <- c.Handle(context.Background(), &js.Message{Msg: msg})
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Stop()

	// 3 доставки (AckWait 2s) + запас.
	for i := 0; i < 3; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("timeout: доставка %d не обработана", i+1)
		}
	}

	// S-12: сообщение не вернулось в основной поток (терминировано).
	time.Sleep(3 * time.Second)
	si, err := e.cons.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if si.NumPending != 0 || si.NumAckPending != 0 {
		t.Fatalf("main stream: NumPending=%d NumAckPending=%d, want 0 (нет петли dead-letter)", si.NumPending, si.NumAckPending)
	}

	// FR-005, S-9: payload и метаданные dead-letter (причина, попытки = 3).
	stream, err := e.js.Stream(context.Background(), e.dlStream)
	if err != nil {
		t.Fatalf("dead-letter стрим %s: %v", e.dlStream, err)
	}
	msg, err := stream.GetLastMsgForSubject(context.Background(), dlSubject)
	if err != nil {
		t.Fatalf("нет сообщений в dead-letter: %v", err)
	}
	if msg == nil {
		t.Fatal("dead-letter сообщение отсутствует")
	}

	if string(msg.Data) != string(raw) {
		t.Fatalf("payload dead-letter не совпадает с исходным")
	}
	if got := msg.Header.Get("Nats-Msg-Id"); got != evID {
		t.Fatalf("Nats-Msg-Id dead-letter = %q, want %q", got, evID)
	}
	if got := msg.Header.Get("Nats-Dead-Letter"); got != subject {
		t.Fatalf("Nats-Dead-Letter = %q, want %q (исходный subject)", got, subject)
	}
	if got := msg.Header.Get("Nats-Reason"); got == "" {
		t.Fatal("причина отказа отсутствует в dead-letter метаданных (S-9)")
	}
	if got := msg.Header.Get("Nats-Attempts"); got != "3" {
		t.Fatalf("Nats-Attempts = %q, want \"3\" (число попыток, S-9)", got)
	}
}

// S-10 (FR-006): после обработки (COMMIT → ACK) сообщение не передоставляется.
func TestIntegrationAckAcceptedAfterHandle(t *testing.T) {
	e := connect(t)
	evID := "77777777-0000-4000-8000-000000000003"
	e.publish(t, evID, 1)

	is := inbox.NewMemStore()
	c := consumer.New(is, func(ctx context.Context, payload []byte) error { return nil }, consumer.Config{})

	done := make(chan error, 1)
	cc, err := e.cons.Consume(func(msg jetstream.Msg) {
		done <- c.Handle(context.Background(), &js.Message{Msg: msg})
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Stop()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout: сообщение не обработано")
	}

	// Убеждаемся, что дубль не доставлен повторно (идемпотентность + ack после commit).
	time.Sleep(4 * time.Second)
	si, err := e.cons.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if si.NumAckPending != 0 {
		t.Fatalf("NumAckPending = %d, want 0 (ack принят)", si.NumAckPending)
	}
	if got, _ := is.Get(context.Background(), evID); got == nil {
		t.Fatal("запись inbox отсутствует после обработки")
	}
}

// Вспомогательное.
