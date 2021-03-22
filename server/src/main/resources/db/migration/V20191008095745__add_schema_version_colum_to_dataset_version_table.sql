ALTER TABLE public_dataset_versions
    ADD COLUMN schema_version TEXT NOT NULL DEFAULT '';

update public_dataset_versions set schema_version='1.0' WHERE created_at < '2019-09-11';
update public_dataset_versions set schema_version='2.0' WHERE created_at < '2019-09-20' and created_at > '2019-09-11';
update public_dataset_versions set schema_version='3.0' WHERE created_at > '2019-09-20';

ALTER TABLE public_dataset_versions
    ALTER COLUMN schema_version DROP DEFAULT;