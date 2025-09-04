CREATE TABLE helix_locks
(
    id           bigint unsigned                          NOT NULL AUTO_INCREMENT,
    lock_key     VARCHAR(255)                             NOT NULL,
    owner_id     VARCHAR(255)                             NOT NULL,
    expires_at   TIMESTAMP                                NOT NULL,
    status       ENUM ('active', 'inactive', 'deletable') NOT NULL DEFAULT 'active',
    `created_at` datetime                                 NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` datetime                                 NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `lock_key_status_unique_key` (`lock_key`, `status`),
    KEY `lock_key_ids` (`lock_key`)
);