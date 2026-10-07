CREATE TABLE IF NOT EXISTS scores (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id TEXT             NOT NULL,
    score          DOUBLE PRECISION NOT NULL,
    fraud_flag     INTEGER          NOT NULL,
    created_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scores_id_desc   ON scores (id DESC);
CREATE INDEX IF NOT EXISTS idx_scores_fraud_flag ON scores (fraud_flag);
