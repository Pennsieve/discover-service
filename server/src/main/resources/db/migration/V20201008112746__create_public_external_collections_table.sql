CREATE TABLE public_external_publications (
    doi TEXT NOT NULL,
    dataset_id INT NOT NULL,
    version  INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (doi, dataset_id, version),

    CONSTRAINT public_dataset_versions_fk FOREIGN KEY (dataset_id, version)
    REFERENCES public_dataset_versions (dataset_id, version)
    ON DELETE CASCADE
);

CREATE TRIGGER public_external_publications_updated_at
BEFORE UPDATE ON public_external_publications
FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
