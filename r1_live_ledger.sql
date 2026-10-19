-- R1/LINE live staging ledger schema. Schema only: does not place orders.
-- Run against a backed-up PostgreSQL database after review.
BEGIN;
CREATE TABLE IF NOT EXISTS live_order_intents (
 id BIGSERIAL PRIMARY KEY,
 source TEXT NOT NULL CHECK (source IN ('R1','LINE')),
 signal_id TEXT NOT NULL,
 symbol TEXT NOT NULL,
 position_side TEXT NOT NULL CHECK (position_side IN ('LONG','SHORT')),
 intended_margin_usdt NUMERIC(20,8) NOT NULL CHECK (intended_margin_usdt > 0 AND intended_margin_usdt <= 10),
 leverage INTEGER NOT NULL CHECK (leverage BETWEEN 1 AND 8),
 entry_price NUMERIC(30,12) NOT NULL CHECK (entry_price > 0),
 stop_price NUMERIC(30,12) NOT NULL CHECK (stop_price > 0),
 state TEXT NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING','SUBMITTING','ACKNOWLEDGED','PROTECTED','CLOSING','CLOSED','REJECTED','UNKNOWN')),
 exchange_order_id TEXT,
 exchange_position_id TEXT,
 client_order_id TEXT UNIQUE,
 last_error TEXT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 UNIQUE(source, signal_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS live_order_intents_exchange_order_uidx
 ON live_order_intents(exchange_order_id) WHERE exchange_order_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS live_order_events (
 id BIGSERIAL PRIMARY KEY,
 intent_id BIGINT NOT NULL REFERENCES live_order_intents(id),
 event_type TEXT NOT NULL,
 event_data JSONB NOT NULL DEFAULT '{}'::jsonb,
 observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS live_order_events_intent_time_idx ON live_order_events(intent_id,observed_at);
CREATE TABLE IF NOT EXISTS live_risk_state (
 singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
 enabled BOOLEAN NOT NULL DEFAULT FALSE,
 kill_switch BOOLEAN NOT NULL DEFAULT TRUE,
 reason TEXT NOT NULL DEFAULT 'Awaiting live safety verification',
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO live_risk_state(singleton, enabled, kill_switch)
 VALUES(TRUE,FALSE,TRUE) ON CONFLICT(singleton) DO NOTHING;
COMMIT;
