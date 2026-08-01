CREATE TABLE IF NOT EXISTS inbox_events (
    id                uuid PRIMARY KEY,
    event_id          uuid NOT NULL UNIQUE,
    aggregate_id      text NOT NULL,
    aggregate_version bigint NOT NULL CHECK (aggregate_version >= 1),
    topic             text NOT NULL,
    payload           jsonb NOT NULL,
    status            text NOT NULL DEFAULT 'received'
                      CHECK (status IN ('received', 'processing', 'processed', 'failed')),
    attempts          integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at        timestamptz NOT NULL DEFAULT now(),
    processed_at      timestamptz
);

CREATE INDEX IF NOT EXISTS idx_inbox_aggregate ON inbox_events (aggregate_id, aggregate_version);
