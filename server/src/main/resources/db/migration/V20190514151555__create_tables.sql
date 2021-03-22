CREATE EXTENSION IF NOT EXISTS hstore;

CREATE TABLE public_datasets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    source_organization_id INT NOT NULL,
    source_dataset_id INT NOT NULL,
    owner_id INT NOT NULL,
    description VARCHAR(255) NOT NULL,
    license VARCHAR(255) NOT NULL,
    tags VARCHAR(255) ARRAY NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public_dataset_versions (
    dataset_id INT NOT NULL REFERENCES public_datasets(id) ON DELETE CASCADE,
    version INT NOT NULL,
    doi VARCHAR(255) NOT NULL UNIQUE,
    total_size BIGINT NOT NULL,
    contributors VARCHAR(255) ARRAY NOT NULL,
    model_count HSTORE NOT NULL,
    file_count INT NOT NULL,
    record_count INT NOT NULL,
    uri VARCHAR(255) NOT NULL,
    status VARCHAR(255) NOT NULL,
    banner VARCHAR(255),
    readme VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dataset_id, version)
);
