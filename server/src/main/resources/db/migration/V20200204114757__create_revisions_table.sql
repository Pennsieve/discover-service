CREATE TABLE revisions (
    id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL,
    version INT NOT NULL,
    revision INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE revisions
ADD CONSTRAINT public_dataset_versions_fk FOREIGN KEY (dataset_id, version)
REFERENCES public_dataset_versions (dataset_id, version)
ON DELETE CASCADE;

CREATE TRIGGER revisions_updated_at
BEFORE UPDATE ON revisions
FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
