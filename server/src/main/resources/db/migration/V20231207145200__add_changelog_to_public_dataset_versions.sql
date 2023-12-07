/*
** add `changelog` column to public_dataset_versions table
*/
ALTER TABLE public_dataset_versions ADD COLUMN changelog VARCHAR(255);
