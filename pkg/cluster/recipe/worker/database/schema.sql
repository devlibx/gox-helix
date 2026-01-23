CREATE TABLE helix_workers (
    id BIGINT AUTO_INCREMENT,
    worker_id VARCHAR(64) NOT NULL,
    domain VARCHAR(64) NOT NULL,
    status TINYINT NOT NULL DEFAULT 1,
    last_heartbeat_at datetime NOT NULL,
    created_at datetime NOT NULL,
    updated_at datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id, created_at),
    UNIQUE KEY uidx_domain_worker_id (domain, worker_id, created_at)
)
PARTITION BY RANGE (TO_DAYS(created_at)) (
    PARTITION p2025_11 VALUES LESS THAN (739951),
    PARTITION p2025_12 VALUES LESS THAN (739982),
    PARTITION p2026_01 VALUES LESS THAN (740013),
    PARTITION p2026_02 VALUES LESS THAN (740041),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);