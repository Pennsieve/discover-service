CREATE UNIQUE INDEX public_dataset_versions_idx
ON public_dataset_versions (dataset_id, version);

ALTER TABLE public_dataset_versions
ADD CONSTRAINT public_dataset_versions_pk PRIMARY KEY
USING INDEX public_dataset_versions_idx;

ALTER TABLE public_contributors
ADD CONSTRAINT public_dataset_versions_fk FOREIGN KEY (dataset_id, version)
REFERENCES public_dataset_versions (dataset_id, version);
