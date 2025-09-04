CREATE TABLE helix_locks (
    lock_key VARCHAR(255) PRIMARY KEY,
    owner_id VARCHAR(255) NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    status ENUM('active', 'inactive', 'deletable') NOT NULL DEFAULT 'active'
);