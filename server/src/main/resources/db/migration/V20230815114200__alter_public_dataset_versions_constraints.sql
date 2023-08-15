/*
** drop the uniqueness constrain on the s3_key
*/
ALTER TABLE public_dataset_versions
  DROP CONSTRAINT public_dataset_versions_s3_key_key;

/*
** drop the s3_key check looking for number/number/
*/
ALTER TABLE public_dataset_versions
  DROP CONSTRAINT public_dataset_versions_s3_key_check;

/*
** add a check that depends on the value of `migrated`
**   when migrated is true, then s3_key should look like number/
**   when migrated if false, then s3_key should look like number/number/
*/
ALTER TABLE public_dataset_versions
  ADD CHECK((migrated = false AND s3_key ~ '^[\d]+/[\d]+/$') OR (migrated = true AND s3_key ~ '^[\d]+/$'));

/*
** drop `checksum` column from public_files_table
*/
ALTER TABLE public_file_versions DROP COLUMN checksum;
