CREATE TABLE public_collections (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    source_collection_id INT NOT NULL,
    dataset_id INT NOT NULL,
    version  INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER public_collections_updated_at
BEFORE UPDATE ON public_collections
FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

ALTER TABLE public_collections
ADD CONSTRAINT public_dataset_versions_fk FOREIGN KEY (dataset_id, version)
REFERENCES public_dataset_versions (dataset_id, version)
ON DELETE CASCADE;