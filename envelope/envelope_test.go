package envelope

import (
	"encoding/json"
	"testing"
	"time"
)

func validEnvelope() *Envelope {
	return &Envelope{
		EventID:          "11111111-1111-4111-8111-111111111111",
		EventType:        "ecom.catalog.offer.updated.v1",
		SchemaVersion:    1,
		AggregateID:      "offer:42",
		AggregateVersion: 2,
		OccurredAt:       time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC),
		Producer:         "pim",
		Payload:          json.RawMessage(`{"price":100}`),
	}
}

func TestValidateOK(t *testing.T) {
	if err := validEnvelope().Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateErrors(t *testing.T) {
	cases := []struct {
		name string
		mut  func(e *Envelope)
		want string
	}{
		{"event_id не uuid", func(e *Envelope) { e.EventID = "evt-001" }, "event_id должен быть uuid"},
		{"пустой event_id", func(e *Envelope) { e.EventID = "" }, "event_id должен быть uuid"},
		{"event_type с env", func(e *Envelope) { e.EventType = "ecom.test.catalog.offer.updated.v1" }, "event_type должен быть ecom.<domain>.<aggregate>.<event>.v1"},
		{"schema_version 0", func(e *Envelope) { e.SchemaVersion = 0 }, "schema_version должен быть >= 1"},
		{"пустой aggregate_id", func(e *Envelope) { e.AggregateID = "" }, "aggregate_id не может быть пустым"},
		{"aggregate_version 0", func(e *Envelope) { e.AggregateVersion = 0 }, "aggregate_version должен быть >= 1"},
		{"нулевой occurred_at", func(e *Envelope) { e.OccurredAt = time.Time{} }, "occurred_at не может быть нулевым"},
		{"пустой producer", func(e *Envelope) { e.Producer = "" }, "producer не может быть пустым"},
		{"невалидный payload", func(e *Envelope) { e.Payload = json.RawMessage(`{`) }, "payload должен быть валидным"},
		{"пустой payload", func(e *Envelope) { e.Payload = nil }, "payload должен быть валидным"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := validEnvelope()
			tc.mut(e)
			err := e.Validate()
			if err == nil {
				t.Fatal("Validate() = nil, want error")
			}
			if got := err.Error(); len(tc.want) > 0 && !contains(got, tc.want) {
				t.Fatalf("Validate() = %q, want содержит %q", got, tc.want)
			}
		})
	}
}

func TestParseMarshalRoundTrip(t *testing.T) {
	e := validEnvelope()
	corr := "corr-1"
	caus := "caus-2"
	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	e.CorrelationID = &corr
	e.CausationID = &caus
	e.Traceparent = &tp

	raw, err := e.Marshal()
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	got, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got.EventID != e.EventID || got.AggregateVersion != e.AggregateVersion {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	if got.CorrelationID == nil || *got.CorrelationID != corr {
		t.Fatalf("correlation_id не сохранился: %v", got.CorrelationID)
	}
	if got.Traceparent == nil || *got.Traceparent != tp {
		t.Fatalf("traceparent не сохранился: %v", got.Traceparent)
	}
}

func TestParseInvalidJSON(t *testing.T) {
	if _, err := Parse([]byte("{invalid")); err == nil {
		t.Fatal("Parse() = nil, want error")
	}
}

func TestEnvelopeHas11Fields(t *testing.T) {
	// EVENTS-001 §1: 11 полей конверта — защита от регрессии контракта.
	corr, caus, tp := "corr-1", "caus-2", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	e := validEnvelope()
	e.CorrelationID = &corr
	e.CausationID = &caus
	e.Traceparent = &tp
	raw, err := e.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"event_id", "event_type", "schema_version", "aggregate_id", "aggregate_version",
		"occurred_at", "producer", "correlation_id", "causation_id", "traceparent", "payload",
	}
	if len(m) != len(want) {
		t.Fatalf("полей в конверте %d, want %d", len(m), len(want))
	}
	for _, f := range want {
		if _, ok := m[f]; !ok {
			t.Errorf("отсутствует поле конверта %q", f)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
