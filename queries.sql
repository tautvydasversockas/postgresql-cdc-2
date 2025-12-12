CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR NOT NULL, 
    aggregate_id VARCHAR NOT NULL, 
    event_type VARCHAR NOT NULL, 
    payload JSONB NOT NULL,                
    created_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER ROLE postgres WITH REPLICATION;
