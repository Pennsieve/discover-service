/*
** Add a "type" column to the Public Datasets tablet to distinguish the kind of
** dataset being published.
*/
ALTER TABLE public_datasets ADD COLUMN dataset_type TEXT NOT NULL DEFAULT 'research';

/*
** Public Dataset Release
**   This table holds the details of a Release.
*/
CREATE TABLE public_dataset_release (
    id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL,
    dataset_version INT NOT NULL,
    origin TEXT NOT NULL,
    label TEXT NOT NULL,
    marker TEXT NOT NULL,
    repo_url TEXT NOT NULL,
    label_url TEXT,
    marker_url TEXT,
    release_status TEXT,
    asset_file_prefix TEXT,
    asset_file_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dataset_id, dataset_version),
    CONSTRAINT public_dataset_release_fk1
        FOREIGN KEY (dataset_id, dataset_version)
        REFERENCES public_dataset_versions(dataset_id, version)
        ON DELETE CASCADE
);

CREATE TRIGGER public_dataset_release_updated_at
    BEFORE UPDATE ON public_dataset_release
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

/*
** Public Dataset Release Assets
**   This table holds the file listing of the release asset (Zip file).
*/
CREATE TABLE public_dataset_release_assets (
    id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL,
    dataset_version INT NOT NULL,
    release_id INT NOT NULL,
    file TEXT NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    size INT NOT NULL,
    path ltree NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT public_dataset_release_assets_fk1
        FOREIGN KEY (dataset_id, dataset_version)
            REFERENCES public_dataset_versions(dataset_id, version)
            ON DELETE CASCADE,
    CONSTRAINT public_dataset_release_assets_fk2
        FOREIGN KEY (release_id)
            REFERENCES public_dataset_release(id)
            ON DELETE CASCADE
);

CREATE TRIGGER public_dataset_release_assets_updated_at
    BEFORE UPDATE ON public_dataset_release_assets
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
