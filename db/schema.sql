-- Domains: A logical namespace for a set of related resources and tasks.
CREATE TABLE domains (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY udx_name (name)
);

-- Queues (Task Lists): A queue of tasks within a domain.
CREATE TABLE queues (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    domain_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (domain_id) REFERENCES domains(id) ON DELETE CASCADE,
    UNIQUE KEY udx_domain_id_name (domain_id, name)
);

-- Partitions: A unit of work that can be assigned to a worker.
CREATE TABLE partitions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    queue_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE CASCADE,
    UNIQUE KEY udx_queue_id_name (queue_id, name)
);


