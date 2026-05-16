CREATE TABLE leases (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    lease_id BIGINT UNSIGNED NOT NULL,
    prov_address VARCHAR(64) NOT NULL,
    node_address VARCHAR(64) NOT NULL,
    price_denom VARCHAR(32) NOT NULL,
    price_base_value DECIMAL(30, 18) NOT NULL,
    price_quote_value BIGINT UNSIGNED NOT NULL,
    hours INT UNSIGNED NOT NULL,
    max_hours INT UNSIGNED NOT NULL,
    renewal_price_policy VARCHAR(32) NOT NULL,
    start_at DATETIME(6) NOT NULL,
    first_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_lease_snapshot (
        lease_id, prov_address, node_address, price_denom,
        price_base_value, price_quote_value, hours, max_hours,
        renewal_price_policy, start_at
    ),
    INDEX idx_lease_id (lease_id),
    INDEX idx_node_address (node_address),
    INDEX idx_prov_address (prov_address),
    INDEX idx_start_at (start_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;