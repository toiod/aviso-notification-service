-- +goose Up

CREATE TABLE notification_attempts (
    notification_id BIGINT NOT NULL REFERENCES notifications(id),
    attempt_no INTEGER CHECK (attempt_no BETWEEN 1 AND 5),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    outcome TEXT NOT NULL DEFAULT 'started' CHECK (outcome IN ('started', 'success', 'temporary_error', 'permanent_error', 'unknown')),
    error TEXT,

    CHECK (started_at <= finished_at),
    CHECK ((outcome = 'started') = (finished_at IS NULL)),
    PRIMARY KEY (notification_id, attempt_no)
);

-- +goose Down

DROP TABLE notification_attempts;