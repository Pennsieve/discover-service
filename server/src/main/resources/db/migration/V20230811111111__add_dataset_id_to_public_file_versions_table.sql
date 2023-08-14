/*
** add `dataset_id` column to public_file_versions table
*/
ALTER TABLE public_file_versions ADD COLUMN dataset_id INT NOT NULL;

/*
** indexing needs on public_file_versions
*/

/*
** indexing needs on public_dataset_version_files
*/
