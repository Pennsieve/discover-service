/*
** add `checksum` column to public_files_table
*/
ALTER TABLE public_file_versions add COLUMN sha256 TEXT;
