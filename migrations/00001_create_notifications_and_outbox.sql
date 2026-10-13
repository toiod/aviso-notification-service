-- +goose Up

CREATE TABLE notifications (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    recipient TEXT NOT NULL CHECK (BTRIM(recipient) <> ''),
    channel TEXT NOT NULL CHECK (channel = 'telegram'),
    message TEXT NOT NULL
            CHECK (CHAR_LENGTH(message) BETWEEN 1 AND 4096),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processing','sent','failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 5),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ CHECK ((status = 'sent') = (sent_at IS NOT NULL)),
    last_error TEXT,
    next_attempt_at TIMESTAMPTZ
);

CREATE TABLE outbox (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    notification_id BIGINT NOT NULL UNIQUE REFERENCES notifications(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- +goose Down

DROP TABLE outbox;
DROP TABLE notifications;