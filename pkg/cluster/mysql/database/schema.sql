CREATE TABLE helix_nodes
(
    id            bigint unsigned NOT NULL AUTO_INCREMENT,
    cluster_name  VARCHAR(255)    NOT NULL,
    node_uuid     VARCHAR(255)    NOT NULL,
    node_metadata TEXT            NULL,
    last_hb_time  TIMESTAMP       NOT NULL,
    status        TINYINT         NOT NULL DEFAULT 1,
    version       int             NOT NULL DEFAULT 1,
    `created_at`  datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`  datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`, `status`),
    UNIQUE KEY `cluster_node_status_unique` (`cluster_name`, `node_uuid`, `status`),
    KEY `cluster_status_idx` (`cluster_name`, `status`)
) PARTITION BY LIST (`status`) (
    PARTITION p_active VALUES IN (1),
    PARTITION p_inactive VALUES IN (0),
    PARTITION p_deletable VALUES IN (2)
    );

CREATE TABLE helix_cluster
(
    id              bigint unsigned NOT NULL AUTO_INCREMENT,
    cluster         VARCHAR(255)    NOT NULL,
    domain          VARCHAR(255)    NOT NULL,
    tasklist        VARCHAR(255)    NOT NULL,
    metadata        TEXT            NULL,
    partition_count INT UNSIGNED    NOT NULL DEFAULT 1,
    status          TINYINT         NOT NULL DEFAULT 1,
    `created_at`    datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`    datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`, `status`),
    UNIQUE KEY `cluster_domain_tasklist_status_unique` (`cluster`, `domain`, `tasklist`, `status`),
    KEY `cluster_domain_idx` (`cluster`, `domain`)
) PARTITION BY LIST (`status`) (
    PARTITION p_active VALUES IN (1),
    PARTITION p_inactive VALUES IN (0),
    PARTITION p_deletable VALUES IN (2)
    );