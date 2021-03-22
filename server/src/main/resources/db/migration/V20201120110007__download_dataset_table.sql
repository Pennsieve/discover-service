CREATE TABLE dataset_downloads (
    dataset_id INT NOT NULL,
    version INT NOT NULL,
    origin TEXT,
    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT public_dataset_versions_fk FOREIGN KEY (dataset_id, version)
    REFERENCES public_dataset_versions (dataset_id, version)
    ON DELETE CASCADE
    );

