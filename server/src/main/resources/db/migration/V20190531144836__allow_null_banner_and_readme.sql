ALTER TABLE public_dataset_versions
    ALTER COLUMN banner DROP NOT NULL,
    ALTER COLUMN readme DROP NOT NULL;
