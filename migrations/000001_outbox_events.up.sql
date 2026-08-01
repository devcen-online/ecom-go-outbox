CREATE TABLE IF NOT EXISTS outbox_events (
    id                uuid PRIMARY KEY,
    event_id          uuid NOT NULL UNIQUE,
    aggregate_id      text NOT NULL,
    aggregate_version bigint NOT NULL CHECK (aggregate_version >= 1),
    topic             text NOT NULL,
    payload           jsonb NOT NULL,
    status            text NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'published', 'failed')),
    attempts          integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at        timestamptz NOT NULL DEFAULT now(),
    processed_at      timestamptz
);

CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_events (status, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_topic ON outbox_events (topic);
