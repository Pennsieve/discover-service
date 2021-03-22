ALTER TABLE public_files
    ADD COLUMN s3_version TEXT;
ALTER TABLE public_files
    ADD COLUMN source_file_id TEXT;