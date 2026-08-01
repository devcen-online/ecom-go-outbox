// Package envelope реализует эталонный конверт события платформы EVENTS-001 v0.2.0
// (11 полей, ecom-schema-registry/docs/EVENTS.md, сущность Envelope ERD-OB-001).
package envelope

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Envelope — конверт события платформы (EVENTS-001 §1).
// EventID совпадает с Nats-Msg-Id при публикации (ERD-OB-001, INV-2).
type Envelope struct {
	EventID          string          `json:"event_id"`
	EventType        string          `json:"event_type"`
	SchemaVersion    int             `json:"schema_version"`
	AggregateID      string          `json:"aggregate_id"`
	AggregateVersion int64           `json:"aggregate_version"`
	OccurredAt       time.Time       `json:"occurred_at"`
	Producer         string          `json:"producer"`
	CorrelationID    *string         `json:"correlation_id,omitempty"`
	CausationID      *string         `json:"causation_id,omitempty"`
	Traceparent      *string         `json:"traceparent,omitempty"`
	Payload          json.RawMessage `json:"payload"`
}

// Validate проверяет конверт по правилам EVENTS-001 и ERD-OB-001.
func (e *Envelope) Validate() error {
	if !isUUID(e.EventID) {
		return fmt.Errorf("envelope: event_id должен быть uuid, получено %q", e.EventID)
	}
	if !eventTypeRe.MatchString(e.EventType) {
		return fmt.Errorf("envelope: event_type должен быть ecom.<domain>.<aggregate>.<event>.v1, получено %q", e.EventType)
	}
	if e.SchemaVersion < 1 {
		return fmt.Errorf("envelope: schema_version должен быть >= 1, получено %d", e.SchemaVersion)
	}
	if strings.TrimSpace(e.AggregateID) == "" {
		return errors.New("envelope: aggregate_id не может быть пустым")
	}
	if e.AggregateVersion < 1 {
		return fmt.Errorf("envelope: aggregate_version должен быть >= 1, получено %d", e.AggregateVersion)
	}
	if e.OccurredAt.IsZero() {
		return errors.New("envelope: occurred_at не может быть нулевым")
	}
	if strings.TrimSpace(e.Producer) == "" {
		return errors.New("envelope: producer не может быть пустым")
	}
	if !json.Valid(e.Payload) || len(e.Payload) == 0 {
		return errors.New("envelope: payload должен быть валидным непустым JSON")
	}
	return nil
}

// Parse разбирает JSON-байты в конверт и валидирует его.
func Parse(data []byte) (*Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, fmt.Errorf("envelope: невалидный JSON: %w", err)
	}
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return &e, nil
}

// Marshal сериализует конверт в JSON (как хранится в outbox_events.payload / inbox_events.payload).
func (e *Envelope) Marshal() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}

// eventTypeRe: ecom.<domain>.<aggregate>.<event>.vN (без env, EVENTS-001 §1).
var eventTypeRe = regexp.MustCompile(`^ecom\.[a-z0-9][a-z0-9_-]*\.[a-z0-9][a-z0-9_-]*\.[a-z0-9][a-z0-9_-]*\.v[0-9]+$`)

// isUUID проверяет строку формата 8-4-4-4-12 hex (вариант без внешних зависимостей).
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
				return false
			}
		}
	}
	return true
}
