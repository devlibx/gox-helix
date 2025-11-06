CREATE TABLE helix_domain
(
    id              bigint unsigned NOT NULL AUTO_INCREMENT,
    domain          VARCHAR(64)     NOT NULL,
    tasklist        VARCHAR(64)     NOT NULL,
    metadata        TEXT            NULL,
    partition_count INT UNSIGNED    NOT NULL DEFAULT 1,
    status          TINYINT         NOT NULL DEFAULT 1,
    created_at      datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`, `status`),
    UNIQUE KEY `domain_tasklist_status_unique` (`domain`, `tasklist`, `status`)
) PARTITION BY LIST (`status`) (
    PARTITION p_active VALUES IN (1),
    PARTITION p_inactive VALUES IN (0),
    PARTITION p_deletable VALUES IN (2)
    );

CREATE TABLE helix_worker
(
    id           bigint unsigned NOT NULL AUTO_INCREMENT,
    domain       VARCHAR(64)     NOT NULL,
    unique_id    VARCHAR(64)     NOT NULL,
    metadata     TEXT            NULL,
    last_hb_time TIMESTAMP       NOT NULL,
    status       TINYINT         NOT NULL DEFAULT 1,
    version      int unsigned    NOT NULL DEFAULT 1,
    created_at   datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`, `status`),
    UNIQUE KEY `cluster_node_status_unique` (`domain`, `unique_id`, `status`),
    KEY `cluster_status_idx` (`domain`, `status`)
) PARTITION BY LIST (`status`) (
    PARTITION p_active VALUES IN (1),
    PARTITION p_inactive VALUES IN (0),
    PARTITION p_deletable VALUES IN (2)
    );


CREATE TABLE helix_locks
(
    id           bigint unsigned NOT NULL AUTO_INCREMENT,
    domain       VARCHAR(64)     NOT NULL,
    lock_key     VARCHAR(255)    NOT NULL,
    owner_id     VARCHAR(64)     NOT NULL,
    expires_at   TIMESTAMP       NOT NULL,
    epoch        bigint unsigned NOT NULL DEFAULT 0,
    status       TINYINT         NOT NULL DEFAULT 1,
    `created_at` datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `lock_key_status_unique_key` (`domain`, `lock_key`, `status`),
    KEY `lock_key_ids` (`lock_key`)
);