/*
** Table: public_file_versions
*/
CREATE TABLE public_file_versions (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  file_type TEXT NOT NULL,
  size BIGINT NOT NULL,
  source_package_id TEXT NOT NULL,
  source_file_uuid UUID NOT NULL,
  checksum JSONB,
  s3_key TEXT NOT NULL,
  s3_version TEXT NOT NULL,
  path ltree NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER public_file_versions_updated_at
BEFORE UPDATE ON public_file_versions
FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

/*
** Table: public_dataset_version_files
*/
CREATE TABLE public_dataset_version_files (
  dataset_id INT NOT NULL,
  dataset_version INT NOT NULL,
  file_id INT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT public_dataset_versions_files_fk1
      FOREIGN KEY (dataset_id, dataset_version)
      REFERENCES public_dataset_versions(dataset_id, version)
      ON DELETE CASCADE,
  CONSTRAINT public_dataset_versions_files_fk2
      FOREIGN KEY (file_id)
      REFERENCES public_file_versions(id)
      ON DELETE CASCADE
);

CREATE TRIGGER public_dataset_version_files_updated_at
BEFORE UPDATE ON public_dataset_version_files
FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
