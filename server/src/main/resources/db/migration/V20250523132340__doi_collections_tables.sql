/*
** Public Dataset DOI Collections
**   This table holds the details of a DOI Collection.
** Postgres does not enforce ARRAY[4] size. Just for documentation.
*/
CREATE TABLE public_dataset_doi_collections
(
    id              SERIAL PRIMARY KEY,
    dataset_id      INT           NOT NULL,
    dataset_version INT           NOT NULL,
    banners         TEXT ARRAY[4] NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dataset_id, dataset_version),
    CONSTRAINT public_dataset_doi_collections_fk
        FOREIGN KEY (dataset_id, dataset_version)
            REFERENCES public_dataset_versions (dataset_id, version)
            ON DELETE CASCADE
);

CREATE TRIGGER public_dataset_doi_collections_updated_at
    BEFORE UPDATE
    ON public_dataset_doi_collections
    FOR EACH ROW
EXECUTE PROCEDURE update_updated_at_column();

/*
** Public Dataset DOI Collection DOIs
**   This table holds the DOI listing of the collection.
*/
CREATE TABLE public_dataset_doi_collection_dois
(
    id              SERIAL PRIMARY KEY,
    dataset_id      INT          NOT NULL,
    dataset_version INT          NOT NULL,
    doi             VARCHAR(255) NOT NULL,
    position        INT          NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dataset_id, dataset_version, doi),
    CONSTRAINT public_dataset_doi_collection_dois_fk
        FOREIGN KEY (dataset_id, dataset_version)
            REFERENCES public_dataset_versions (dataset_id, version)
            ON DELETE CASCADE
);

CREATE TRIGGER public_dataset_doi_collection_dois_updated_at
    BEFORE UPDATE
    ON public_dataset_doi_collection_dois
    FOR EACH ROW
EXECUTE PROCEDURE update_updated_at_column();