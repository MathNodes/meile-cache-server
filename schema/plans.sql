CREATE TABLE Plans (
    id BIGINT PRIMARY KEY,
    provider_address VARCHAR(255),
    duration VARCHAR(64),
    gigabytes BIGINT,
    denom VARCHAR(255),
    amount BIGINT,
    status VARCHAR(20),
    status_at DATETIME(6)
);