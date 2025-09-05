-- Migration script to change status field from ENUM to TINYINT
-- Run this script to recreate the table with the new schema

-- Drop the existing table (WARNING: This will delete all data)
DROP TABLE IF EXISTS helix_locks;

-- Create table with new schema using TINYINT for status
CREATE TABLE helix_locks
(
    id           bigint unsigned                          NOT NULL AUTO_INCREMENT,
    lock_key     VARCHAR(255)                             NOT NULL,
    owner_id     VARCHAR(255)                             NOT NULL,
    expires_at   TIMESTAMP                                NOT NULL,
    epoch        bigint                                   NOT NULL DEFAULT 0,
    status       TINYINT                                  NOT NULL DEFAULT 1,
    `created_at` datetime                                 NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` datetime                                 NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `lock_key_status_unique_key` (`lock_key`, `status`),
    KEY `lock_key_ids` (`lock_key`)
);

-- Status constants reference:
-- active = 1
-- inactive = 0  
-- deletable = 2