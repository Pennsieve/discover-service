CREATE EXTENSION IF NOT EXISTS ltree;

CREATE TABLE public_files (
    id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL,
    version INT NOT NULL,
    name TEXT NOT NULL,
    file_type TEXT NOT NULL,
    size BIGINT NOT NULL,
    s3_key TEXT NOT NULL,
    path ltree NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT public_dataset_versions_fk
      FOREIGN KEY (dataset_id, version)
      REFERENCES public_dataset_versions(dataset_id, version)
      ON DELETE CASCADE
);

CREATE INDEX public_files_path_idx ON public_files USING GIST(path);

CREATE TRIGGER public_files_updated_at
BEFORE UPDATE ON public_files
FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
