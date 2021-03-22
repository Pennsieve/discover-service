ALTER TABLE public_dataset_versions
    ADD COLUMN dataset_downloads_counter INT NOT NULL DEFAULT 0;

ALTER TABLE public_dataset_versions
    ADD COLUMN file_downloads_counter INT NOT NULL DEFAULT 0;