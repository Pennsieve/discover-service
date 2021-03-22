ALTER TABLE public_dataset_versions
    -- First, let's remove the previous regex check
    DROP CONSTRAINT public_dataset_versions_s3_key_check,
    -- Then remove the uniqueness constraint
    DROP CONSTRAINT public_dataset_versions_s3_key_key,
    -- Finally add the new regex check
    ADD CHECK(s3_key ~ '^versioned/[\d]+/$');

