ALTER TABLE public_dataset_versions
    ALTER COLUMN s3_bucket DROP DEFAULT,
    ALTER COLUMN s3_key DROP DEFAULT,
    ADD CHECK(s3_key ~ '^[\d]+/[\d]+/$'),
    ADD UNIQUE(s3_key);
