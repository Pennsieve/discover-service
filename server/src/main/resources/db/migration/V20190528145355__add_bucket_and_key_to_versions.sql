ALTER TABLE public_dataset_versions
    DROP COLUMN uri,
    ADD COLUMN s3_bucket TEXT NOT NULL DEFAULT '',
    ADD COLUMN s3_key TEXT NOT NULL DEFAULT '';
