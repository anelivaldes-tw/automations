-- PAD 213 PoC - Database Schema

CREATE TABLE IF NOT EXISTS subscriptions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         TEXT NOT NULL,
    subscription_type TEXT NOT NULL,  -- 'BILL', 'P2P', 'MOCK'
    status          TEXT NOT NULL DEFAULT 'ACTIVE',
    frequency       TEXT NOT NULL DEFAULT 'DAILY',
    destination_id  TEXT NOT NULL,
    amount          NUMERIC(15,2) NOT NULL,
    metadata        JSONB DEFAULT '{}',
    next_execution_at TIMESTAMPTZ NOT NULL,
    retry_count     INT DEFAULT 0,
    max_retries     INT DEFAULT 3,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS payment_execution_queue (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(id),
    subscription_type TEXT NOT NULL,
    due_at          TIMESTAMPTZ NOT NULL,
    status          TEXT NOT NULL DEFAULT 'READY',  -- READY, PROCESSING, DONE, FAILED
    attempt         INT DEFAULT 1,
    workflow_id     TEXT,
    locked_at       TIMESTAMPTZ,
    locked_by       TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS notification_outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(id),
    event_type      TEXT NOT NULL,  -- 'PAYMENT_SUCCEEDED', 'PAYMENT_FAILED', 'REMINDER'
    delivery_class  TEXT NOT NULL DEFAULT 'IMMEDIATE',  -- 'IMMEDIATE', 'DELAYED'
    payload         JSONB NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING, PUBLISHED
    idempotency_key TEXT UNIQUE NOT NULL,
    scheduled_for   TIMESTAMPTZ,
    published_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_queue_ready ON payment_execution_queue(due_at) WHERE status = 'READY';
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON notification_outbox(delivery_class, status) WHERE status = 'PENDING';
