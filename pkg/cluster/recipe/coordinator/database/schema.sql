CREATE TABLE helix_worker_partition_mapping
(
    id         bigint unsigned NOT NULL AUTO_INCREMENT,
    domain     VARCHAR(64)     NOT NULL,
    tasklist   VARCHAR(64)     NOT NULL,
    owner_id   VARCHAR(64)     NOT NULL,
    status     tinyint         NOT NULL default 'unassigned',
    metadata   TEXT            NULL,
    created_at datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    KEY `domain_tasklist_key` (`domain`, `tasklist`)
);
