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
