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