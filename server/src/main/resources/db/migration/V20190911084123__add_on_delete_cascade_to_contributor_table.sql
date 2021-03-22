ALTER TABLE public_contributors
DROP CONSTRAINT public_dataset_versions_fk;

ALTER TABLE public_contributors
ADD CONSTRAINT public_dataset_versions_fk FOREIGN KEY (dataset_id, version)
REFERENCES public_dataset_versions (dataset_id, version)
ON DELETE CASCADE;