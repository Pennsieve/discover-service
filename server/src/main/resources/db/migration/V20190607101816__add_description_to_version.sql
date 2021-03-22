ALTER TABLE public_dataset_versions
    ADD COLUMN description TEXT NOT NULL DEFAULT '';

ALTER TABLE public_datasets
    ALTER COLUMN description SET DEFAULT '';

UPDATE public_dataset_versions
    SET description = public_datasets.description
    FROM public_datasets WHERE public_dataset_versions.dataset_id = public_datasets.id;
