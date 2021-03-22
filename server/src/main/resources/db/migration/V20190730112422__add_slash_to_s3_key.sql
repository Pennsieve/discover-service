UPDATE public_dataset_versions
SET s3_key = concat(s3_key, '/');
