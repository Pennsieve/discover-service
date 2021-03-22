ALTER TABLE public_datasets
    ADD COLUMN owner_first_name VARCHAR(255) NOT NULL DEFAULT '';
ALTER TABLE public_datasets
    ADD COLUMN owner_last_name VARCHAR(255) NOT NULL DEFAULT '';
ALTER TABLE public_datasets
    ADD COLUMN owner_orcid VARCHAR(19) NOT NULL DEFAULT '';

update public_datasets set owner_first_name=trim(substring(owner_name from '^[^\s]*')), owner_last_name= trim(substring(owner_name from ' .*$'));
