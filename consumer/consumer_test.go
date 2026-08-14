package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/devcen-online/ecom-go-outbox/inbox"
)

// fakeMsg — тестовое сообщение JetStream.
type fakeMsg struct {
	subject       string
	data          []byte
	headers       map[string]string
	deliveryCount uint64
	acked         bool
	nacked        bool
	termed        bool
	order         *[]string // test-only: общий журнал операций
}

func (m *fakeMsg) Subject() string                { return m.subject }
func (m *fakeMsg) Data() []byte                   { return m.data }
func (m *fakeMsg) HeaderValue(name string) string { return m.headers[name] }
func (m *fakeMsg) DeliveryCount() uint64          { return m.deliveryCount }
func (m *fakeMsg) Ack() error {
	m.acked = true
	*m.order = append(*m.order, "ack")
	return nil
}
func (m *fakeMsg) Nack() error {
	m.nacked = true
	*m.order = append(*m.order, "nack")
	return nil
}
func (m *fakeMsg) Term() error {
	m.termed = true
	*m.order = append(*m.order, "term")
	return nil
}

// fakeDLSink — тестовый приёмник dead-letter (INV-5, FR-005).
type fakeDLSink struct {
	messages []*DeadLetterMessage
}

func (s *fakeDLSink) PublishDeadLetter(ctx context.Context, dl *DeadLetterMessage) error {
	s.messages = append(s.messages, dl)
	return nil
}

// captureLog — перехват логов consumer.
type captureLog struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (c *captureLog) Printf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf.WriteString(fmt.Sprintf(format, args...))
	c.buf.WriteString("\n")
}
func (c *captureLog) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

func envelopeBytes(eventID, eventType, aggID string, ver int64, payload string) []byte {
	b, _ := json.Marshal(map[string]any{
		"event_id":          eventID,
		"event_type":        eventType,
		"schema_version":    1,
		"aggregate_id":      aggID,
		"aggregate_version": ver,
		"occurred_at":       time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC),
		"producer":          "pim",
		"payload":           json.RawMessage(payload),
	})
	return b
}

func newConsumer(t *testing.T, h Handler, after func(), logc *captureLog) (*Consumer, *inbox.MemStore, *[]string) {
	t.Helper()
	order := &[]string{}
	store := inbox.NewMemStore()
	store.CommitHook = func() { *order = append(*order, "commit") }
	return New(store, h, Config{Logger: logc, AfterCommit: after}), store, order
}

// S-3 (FR-002, smoke): дубль Nats-Msg-Id → обработчик вызывается ровно один раз,
// в inbox ровно одна запись, в логе «duplicate ignored» с id. Жизненный цикл
// записи (ERD-OB-001, major-1): после успеха — processed/processed_at,
// attempts = число доставок (INV-4).
func TestDuplicateMsgIDIgnored(t *testing.T) {
	logc := &captureLog{}
	var calls int
	c, store, _ := newConsumer(t, func(ctx context.Context, payload []byte) error {
		calls++
		return nil
	}, nil, logc)

	ctx := context.Background()
	msg := &fakeMsg{
		subject:       "ecom.test.catalog.product.created.v1",
		data:          envelopeBytes("11111111-1111-4111-8111-111111111111", "ecom.catalog.product.created.v1", "product:p-1", 1, `{"name":"p-1"}`),
		headers:       map[string]string{"Nats-Msg-Id": "11111111-1111-4111-8111-111111111111"},
		deliveryCount: 1,
		order:         &[]string{},
	}
	if err := c.Handle(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("вызовов обработчика = %d, want 1", calls)
	}
	if !msg.acked || msg.nacked {
		t.Fatalf("ack=%v nack=%v, want ack без nack", msg.acked, msg.nacked)
	}
	first, err := store.Get(ctx, "11111111-1111-4111-8111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != inbox.StatusProcessed {
		t.Fatalf("status после успеха = %q, want processed (ERD-OB-001)", first.Status)
	}
	if first.ProcessedAt == nil {
		t.Fatal("processed_at не заполнен после успеха (ERD-OB-001)")
	}
	if first.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (число доставок, INV-4)", first.Attempts)
	}

	dup := &fakeMsg{
		subject:       "ecom.test.catalog.product.created.v1",
		data:          msg.data,
		headers:       map[string]string{"Nats-Msg-Id": "11111111-1111-4111-8111-111111111111"},
		deliveryCount: 2,
		order:         &[]string{},
	}
	if err := c.Handle(ctx, dup); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("вызовов обработчика после дубля = %d, want 1", calls)
	}
	if !dup.acked {
		t.Fatal("дубль не подтверждён (ack обязателен — событие уже обработано)")
	}
	if store.Count() != 1 {
		t.Fatalf("записей inbox = %d, want 1", store.Count())
	}
	afterDup, err := store.Get(ctx, "11111111-1111-4111-8111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	if afterDup.Attempts != 2 {
		t.Fatalf("attempts после дубля = %d, want 2 (каждая доставка инкрементирует, INV-4)", afterDup.Attempts)
	}
	if afterDup.Status != inbox.StatusProcessed {
		t.Fatalf("status после дубля = %q, want processed", afterDup.Status)
	}
	logOut := logc.String()
	if !strings.Contains(logOut, "duplicate ignored") || !strings.Contains(logOut, "11111111-1111-4111-8111-111111111111") {
		t.Fatalf("лог не содержит 'duplicate ignored' с id: %q", logOut)
	}
}

// S-4 (FR-002, boundary): разные события одного aggregate_id — не дубли;
// версия 2 обрабатывается после версии 1 и не откатывает её результат (INV-6).
// Событие со версией ≤ применённой (stale) — не применяется: обработчик не
// вызывается, запись фиксируется как processed, read model не откатывается.
func TestDifferentVersionsSameAggregateNotDuplicates(t *testing.T) {
	logc := &captureLog{}
	// read model: последняя применённая версия на агрегат (INV-6).
	var readModel sync.Map
	var calls int
	h := func(ctx context.Context, payload []byte) error {
		calls++
		var ev struct {
			EventID          string `json:"event_id"`
			AggregateID      string `json:"aggregate_id"`
			AggregateVersion int64  `json:"aggregate_version"`
		}
		_ = json.Unmarshal(payload, &ev)
		if last, ok := readModel.Load(ev.AggregateID); ok && last.(int64) >= ev.AggregateVersion {
			t.Errorf("INV-6 нарушен: версия %d <= последней %v для %s", ev.AggregateVersion, last, ev.AggregateID)
		}
		readModel.Store(ev.AggregateID, ev.AggregateVersion)
		return nil
	}
	c, store, _ := newConsumer(t, h, nil, logc)

	ctx := context.Background()
	for _, ver := range []int64{1, 2} {
		evID := fmt.Sprintf("11111111-1111-4111-8111-11111111111%d", ver)
		msg := &fakeMsg{
			subject:       "ecom.test.catalog.product.updated.v1",
			data:          envelopeBytes(evID, "ecom.catalog.product.updated.v1", "product:p-1", ver, `{"v":1}`),
			headers:       map[string]string{"Nats-Msg-Id": evID},
			deliveryCount: 1,
			order:         &[]string{},
		}
		if err := c.Handle(ctx, msg); err != nil {
			t.Fatal(err)
		}
		if !msg.acked {
			t.Fatalf("версия %d не подтверждена", ver)
		}
		ev, err := store.Get(ctx, evID)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Status != inbox.StatusProcessed {
			t.Fatalf("версия %d: status = %q, want processed", ver, ev.Status)
		}
	}
	if calls != 2 {
		t.Fatalf("вызовов обработчика = %d, want 2", calls)
	}
	if last, _ := readModel.Load("product:p-1"); last.(int64) != 2 {
		t.Fatalf("read model версия = %v, want 2 (версия 2 не откатила результат)", last)
	}

	// Stale: новое событие (другой event_id) с версией 1 — уже применено (верс. 2).
	staleID := "22222222-2222-4222-8222-222222222222"
	stale := &fakeMsg{
		subject:       "ecom.test.catalog.product.updated.v1",
		data:          envelopeBytes(staleID, "ecom.catalog.product.updated.v1", "product:p-1", 1, `{"v":0}`),
		headers:       map[string]string{"Nats-Msg-Id": staleID},
		deliveryCount: 1,
		order:         &[]string{},
	}
	if err := c.Handle(ctx, stale); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("вызовов обработчика после stale = %d, want 2 (обработчик не вызывается)", calls)
	}
	if !stale.acked {
		t.Fatal("stale событие не подтверждено")
	}
	if last, _ := readModel.Load("product:p-1"); last.(int64) != 2 {
		t.Fatalf("read model откачена stale событием: версия = %v, want 2 (INV-6)", last)
	}
	staleEv, err := store.Get(ctx, staleID)
	if err != nil {
		t.Fatal(err)
	}
	if staleEv.Status != inbox.StatusProcessed {
		t.Fatalf("stale: status = %q, want processed (доставка зафиксирована)", staleEv.Status)
	}
	logOut := logc.String()
	if !strings.Contains(logOut, "stale event ignored") || !strings.Contains(logOut, staleID) {
		t.Fatalf("лог не содержит 'stale event ignored' с id: %q", logOut)
	}
}

// S-5 (FR-002, negative+boundary): дубль Nats-Msg-Id с другим payload —
// обработчик не вызывается, в inbox остаётся payload версии 1; attempts
// инкрементируется (INV-4), status остаётся processed.
func TestDuplicateWithDifferentPayloadKeepsFirst(t *testing.T) {
	logc := &captureLog{}
	var calls int
	c, store, _ := newConsumer(t, func(ctx context.Context, payload []byte) error {
		calls++
		return nil
	}, nil, logc)

	ctx := context.Background()
	v1 := envelopeBytes("11111111-1111-4111-8111-111111111111", "ecom.catalog.product.created.v1", "product:p-1", 1, `{"version":1}`)
	v2 := envelopeBytes("11111111-1111-4111-8111-111111111111", "ecom.catalog.product.created.v1", "product:p-1", 1, `{"version":2}`)

	if err := c.Handle(ctx, &fakeMsg{subject: "ecom.test.catalog.product.created.v1", data: v1, headers: map[string]string{"Nats-Msg-Id": "11111111-1111-4111-8111-111111111111"}, deliveryCount: 1, order: &[]string{}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Handle(ctx, &fakeMsg{subject: "ecom.test.catalog.product.created.v1", data: v2, headers: map[string]string{"Nats-Msg-Id": "11111111-1111-4111-8111-111111111111"}, deliveryCount: 2, order: &[]string{}}); err != nil {
		t.Fatal(err)
	}

	if calls != 1 {
		t.Fatalf("вызовов обработчика = %d, want 1", calls)
	}
	ev, err := store.Get(ctx, "11111111-1111-4111-8111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(ev.Payload), `"version":2`) {
		t.Fatalf("в inbox сохранён payload версии 2, want версию 1: %s", ev.Payload)
	}
	if !strings.Contains(string(ev.Payload), `"version":1`) {
		t.Fatalf("в inbox не сохранён payload версии 1: %s", ev.Payload)
	}
	if ev.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (INV-4: каждая доставка инкрементирует)", ev.Attempts)
	}
	if ev.Status != inbox.StatusProcessed {
		t.Fatalf("status = %q, want processed", ev.Status)
	}
	if ev.ProcessedAt == nil {
		t.Fatal("processed_at не заполнен")
	}
}

// S-10 (FR-006, smoke): порядок операций commit → ack (INV-3, ADR-001).
func TestAckAfterCommitOrder(t *testing.T) {
	logc := &captureLog{}
	c, _, order := newConsumer(t, func(ctx context.Context, payload []byte) error { return nil }, nil, logc)

	msg := &fakeMsg{
		subject:       "ecom.test.catalog.product.created.v1",
		data:          envelopeBytes("11111111-1111-4111-8111-111111111111", "ecom.catalog.product.created.v1", "product:p-1", 1, `{}`),
		headers:       map[string]string{"Nats-Msg-Id": "11111111-1111-4111-8111-111111111111"},
		deliveryCount: 1,
		order:         order,
	}
	if err := c.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("сообщение не подтверждено")
	}
	got := strings.Join(*order, ",")
	if got != "commit,ack" {
		t.Fatalf("порядок операций = %q, want \"commit,ack\" (FR-006)", got)
	}
}

// S-10 (negative): ошибка обработчика → nack, коммита нет.
func TestHandlerErrorNacksWithoutCommit(t *testing.T) {
	logc := &captureLog{}
	c, _, order := newConsumer(t, func(ctx context.Context, payload []byte) error {
		return context.DeadlineExceeded
	}, nil, logc)

	msg := &fakeMsg{
		subject:       "ecom.test.catalog.product.created.v1",
		data:          envelopeBytes("11111111-1111-4111-8111-111111111111", "ecom.catalog.product.created.v1", "product:p-1", 1, `{}`),
		headers:       map[string]string{"Nats-Msg-Id": "11111111-1111-4111-8111-111111111111"},
		deliveryCount: 1,
		order:         order,
	}
	if err := c.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.nacked || msg.acked {
		t.Fatalf("nack=%v ack=%v, want nack без ack", msg.nacked, msg.acked)
	}
	if strings.Contains(strings.Join(*order, ","), "commit") {
		t.Fatalf("commit выполнен при ошибке обработчика: %v", *order)
	}
}

// S-11 (FR-006+FR-007, negative): сбой между COMMIT и ACK (fault injection) →
// redelivery, повторной обработки нет, read model соответствует однократной.
func TestCrashAfterCommitBeforeAck(t *testing.T) {
	logc := &captureLog{}
	var calls int
	var readModel []int
	// fault injection «crash after commit, before ack» (PRD-033 Q-2, BDD-033#S-11).
	crash := func() { panic("crash after commit, before ack") }
	c, store, _ := newConsumer(t, func(ctx context.Context, payload []byte) error {
		calls++
		readModel = append(readModel, calls)
		return nil
	}, crash, logc)

	ctx := context.Background()
	msg := &fakeMsg{
		subject:       "ecom.test.catalog.product.created.v1",
		data:          envelopeBytes("11111111-1111-4111-8111-111111111111", "ecom.catalog.product.created.v1", "product:p-1", 1, `{}`),
		headers:       map[string]string{"Nats-Msg-Id": "11111111-1111-4111-8111-111111111111"},
		deliveryCount: 1,
		order:         &[]string{},
	}
	func() {
		defer func() { _ = recover() }() // имитация аварийного завершения процесса
		_ = c.Handle(ctx, msg)
	}()
	if msg.acked {
		t.Fatal("ACK отправлен до «краша» — ack должен остаться неотправленным")
	}
	// Запись зафиксирована до «краша»: status processed, attempts = 1 (major-1).
	ev, err := store.Get(ctx, "11111111-1111-4111-8111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Status != inbox.StatusProcessed {
		t.Fatalf("status = %q, want processed (COMMIT выполнен до краша)", ev.Status)
	}

	// Consumer перезапущен: JetStream redelivers то же сообщение (тот же Nats-Msg-Id).
	restarted := New(store, func(ctx context.Context, payload []byte) error {
		calls++
		readModel = append(readModel, calls)
		return nil
	}, Config{Logger: logc})

	if err := restarted.Handle(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("после рестарта сообщение не подтверждено")
	}
	// S-11: обработка не выполнена повторно; read model — однократная.
	if calls != 1 {
		t.Fatalf("вызовов обработчика = %d, want 1 (идемпотентность после redelivery)", calls)
	}
	if len(readModel) != 1 {
		t.Fatalf("read model применена %d раз, want 1", len(readModel))
	}
}

// Сообщение без Nats-Msg-Id — ошибка контракта.
func TestMissingMsgID(t *testing.T) {
	logc := &captureLog{}
	c, _, _ := newConsumer(t, func(ctx context.Context, payload []byte) error { return nil }, nil, logc)
	msg := &fakeMsg{subject: "ecom.test.catalog.product.created.v1", data: []byte(`{}`), headers: map[string]string{}}
	err := c.Handle(context.Background(), msg)
	if err == nil || !strings.Contains(err.Error(), "Nats-Msg-Id") {
		t.Fatalf("Handle() error = %v, want ErrMissingMsgID", err)
	}
}

// INV-2: Nats-Msg-Id != envelope.event_id — ошибка контракта publisher,
// idempotency-ключ был бы неверным → dead-letter + term.
func TestMismatchedEventIDGoesToDeadLetter(t *testing.T) {
	logc := &captureLog{}
	dl := &fakeDLSink{}
	c, store, _ := newConsumer(t, func(ctx context.Context, payload []byte) error { return nil }, nil, logc)
	c.dlSink = dl

	ctx := context.Background()
	headerID := "11111111-1111-4111-8111-111111111111"
	envelopeID := "22222222-2222-4222-8222-222222222222"
	msg := &fakeMsg{
		subject:       "ecom.test.catalog.product.created.v1",
		data:          envelopeBytes(envelopeID, "ecom.catalog.product.created.v1", "product:p-1", 1, `{"name":"p-1"}`),
		headers:       map[string]string{"Nats-Msg-Id": headerID},
		deliveryCount: 1,
		order:         &[]string{},
	}
	if err := c.Handle(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed || msg.acked || msg.nacked {
		t.Fatalf("termed=%v acked=%v nacked=%v, want только term (dead-letter)", msg.termed, msg.acked, msg.nacked)
	}
	if len(dl.messages) != 1 {
		t.Fatalf("dead-letter публикаций = %d, want 1", len(dl.messages))
	}
	dlm := dl.messages[0]
	if dlm.EventID != headerID {
		t.Fatalf("EventID dead-letter = %q, want headerID %s", dlm.EventID, headerID)
	}
	if !strings.Contains(dlm.Reason, "event_id mismatch") {
		t.Fatalf("Reason = %q, want содержит 'event_id mismatch'", dlm.Reason)
	}
	if store.Count() != 0 {
		t.Fatalf("inbox записей = %d, want 0 (mismatch не создаёт запись)", store.Count())
	}
	logOut := logc.String()
	if !strings.Contains(logOut, "event_id mismatch") {
		t.Fatalf("лог не содержит 'event_id mismatch': %q", logOut)
	}
}

// minor-1 (FR-005, negative): невалидный конверт — не ack (повторная доставка
// вернула бы то же самое), а dead-letter + term: тихой потери нет.
func TestInvalidEnvelopeGoesToDeadLetter(t *testing.T) {
	logc := &captureLog{}
	dl := &fakeDLSink{}
	c, store, _ := newConsumer(t, func(ctx context.Context, payload []byte) error { return nil }, nil, logc)
	c.dlSink = dl

	ctx := context.Background()
	msg := &fakeMsg{
		subject:       "ecom.test.catalog.product.created.v1",
		data:          []byte(`{"event_id":`), // битый JSON — не валидный конверт
		headers:       map[string]string{"Nats-Msg-Id": "11111111-1111-4111-8111-111111111111"},
		deliveryCount: 1,
		order:         &[]string{},
	}
	if err := c.Handle(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed || msg.acked || msg.nacked {
		t.Fatalf("termed=%v acked=%v nacked=%v, want только term (dead-letter, без ack)", msg.termed, msg.acked, msg.nacked)
	}
	if len(dl.messages) != 1 {
		t.Fatalf("dead-letter публикаций = %d, want 1", len(dl.messages))
	}
	dlm := dl.messages[0]
	if dlm.EventID != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("EventID dead-letter = %q", dlm.EventID)
	}
	if dlm.Subject != "ecom.test.catalog.product.created.v1.dl" {
		t.Fatalf("Subject dead-letter = %q, want '<subject>.dl'", dlm.Subject)
	}
	if !strings.Contains(dlm.Reason, "invalid envelope") {
		t.Fatalf("Reason = %q, want содержит 'invalid envelope'", dlm.Reason)
	}
	if string(dlm.Payload) != string(msg.data) {
		t.Fatalf("payload dead-letter не совпадает с исходным")
	}
	if store.Count() != 0 {
		t.Fatalf("невалидный конверт не должен создавать запись inbox, записей = %d", store.Count())
	}
	logOut := logc.String()
	if !strings.Contains(logOut, "invalid envelope") {
		t.Fatalf("лог не содержит 'invalid envelope': %q", logOut)
	}

	// Без настроенного DeadLetterSink — терминация без публикации, не ack и не nack.
	c2, _, _ := newConsumer(t, func(ctx context.Context, payload []byte) error { return nil }, nil, logc)
	msg2 := &fakeMsg{
		subject:       "ecom.test.catalog.product.created.v1",
		data:          []byte(`not-json`),
		headers:       map[string]string{"Nats-Msg-Id": "33333333-3333-4333-8333-333333333333"},
		deliveryCount: 1,
		order:         &[]string{},
	}
	if err := c2.Handle(ctx, msg2); err != nil {
		t.Fatal(err)
	}
	if !msg2.termed || msg2.acked || msg2.nacked {
		t.Fatalf("без sink: termed=%v acked=%v nacked=%v, want только term", msg2.termed, msg2.acked, msg2.nacked)
	}
}

// S-8 (FR-004, boundary): MaxDeliver = 1 → первый же nack отправляет в dead-letter,
// сообщение не передоставлено повторно.
func TestMaxDeliverOneSendsToDeadLetter(t *testing.T) {
	logc := &captureLog{}
	dl := &fakeDLSink{}
	c, _, order := newConsumer(t, func(ctx context.Context, payload []byte) error {
		return context.DeadlineExceeded
	}, nil, logc)
	c.maxDeliver = 1
	c.dlSink = dl

	msg := &fakeMsg{
		subject:       "ecom.test.catalog.product.updated.v1",
		data:          envelopeBytes("11111111-1111-4111-8111-111111111111", "ecom.catalog.product.updated.v1", "product:p-1", 1, `{}`),
		headers:       map[string]string{"Nats-Msg-Id": "11111111-1111-4111-8111-111111111111"},
		deliveryCount: 1,
		order:         order,
	}
	if err := c.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed || msg.nacked || msg.acked {
		t.Fatalf("termed=%v nacked=%v acked=%v, want только term (без redelivery)", msg.termed, msg.nacked, msg.acked)
	}
	if len(dl.messages) != 1 {
		t.Fatalf("dead-letter публикаций = %d, want 1", len(dl.messages))
	}
	assertDeadLetter(t, dl.messages[0], msg, "context deadline exceeded")
}

// S-9 (FR-004, FR-005, negative): исчерпание MaxDeliver = 3 → dead-letter с
// исходным payload, причиной отказа и числом попыток 3; до порога — nack.
func TestMaxDeliverExhaustedDeadLetter(t *testing.T) {
	logc := &captureLog{}
	dl := &fakeDLSink{}
	c, _, _ := newConsumer(t, func(ctx context.Context, payload []byte) error {
		return context.DeadlineExceeded
	}, nil, logc)
	c.maxDeliver = 3
	c.dlSink = dl

	for _, delivery := range []uint64{1, 2, 3} {
		order := &[]string{}
		msg := &fakeMsg{
			subject:       "ecom.test.catalog.product.updated.v1",
			data:          envelopeBytes("11111111-1111-4111-8111-111111111111", "ecom.catalog.product.updated.v1", "product:p-1", 1, `{"price":99}`),
			headers:       map[string]string{"Nats-Msg-Id": "11111111-1111-4111-8111-111111111111"},
			deliveryCount: delivery,
			order:         order,
		}
		if err := c.Handle(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
		if delivery < 3 {
			if !msg.nacked || msg.termed {
				t.Fatalf("доставка %d: nacked=%v termed=%v, want nack (redelivery по AckWait)", delivery, msg.nacked, msg.termed)
			}
			if len(dl.messages) != 0 {
				t.Fatalf("доставка %d: dead-letter публикаций %d, want 0 до исчерпания", delivery, len(dl.messages))
			}
			continue
		}
		if !msg.termed || msg.nacked {
			t.Fatalf("доставка %d (исчерпание): termed=%v nacked=%v, want term (dead-letter, не redelivery)", delivery, msg.termed, msg.nacked)
		}
	}

	if len(dl.messages) != 1 {
		t.Fatalf("dead-letter публикаций = %d, want 1", len(dl.messages))
	}
	dlm := dl.messages[0]
	if dlm.Attempts != 3 {
		t.Fatalf("Attempts dead-letter = %d, want 3 (метаданные, S-9)", dlm.Attempts)
	}
	if dlm.EventID != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("EventID dead-letter = %q", dlm.EventID)
	}
	if dlm.OriginalSubject != "ecom.test.catalog.product.updated.v1" {
		t.Fatalf("OriginalSubject dead-letter = %q", dlm.OriginalSubject)
	}
	if dlm.Subject != "ecom.test.catalog.product.updated.v1.dl" {
		t.Fatalf("Subject dead-letter = %q, want '<subject>.dl'", dlm.Subject)
	}
	if !strings.Contains(string(dlm.Payload), `"price":99`) {
		t.Fatalf("payload dead-letter не совпадает с исходным: %s", dlm.Payload)
	}
	if !strings.Contains(dlm.Reason, "context deadline exceeded") {
		t.Fatalf("причина отказа в dead-letter = %q", dlm.Reason)
	}
}

// assertDeadLetter проверяет метаданные dead-letter для S-8.
func assertDeadLetter(t *testing.T, dl *DeadLetterMessage, src *fakeMsg, wantReason string) {
	t.Helper()
	if dl == nil {
		t.Fatal("dead-letter сообщение отсутствует")
	}
	if dl.EventID != src.HeaderValue("Nats-Msg-Id") {
		t.Fatalf("EventID = %q, want %q", dl.EventID, src.HeaderValue("Nats-Msg-Id"))
	}
	if dl.Attempts != src.deliveryCount {
		t.Fatalf("Attempts = %d, want %d", dl.Attempts, src.deliveryCount)
	}
	if !strings.Contains(dl.Reason, wantReason) {
		t.Fatalf("Reason = %q, want содержит %q", dl.Reason, wantReason)
	}
	if string(dl.Payload) != string(src.data) {
		t.Fatalf("payload dead-letter не совпадает с исходным")
	}
}
