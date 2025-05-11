CREATE TABLE plan_info (
    id BIGINT PRIMARY KEY,
    provider_address VARCHAR(255),
    duration VARCHAR(64),
    gigabytes BIGINT,
    denom VARCHAR(255),
    amount BIGINT,
    status VARCHAR(50),
    status_at DATETIME(6)
);