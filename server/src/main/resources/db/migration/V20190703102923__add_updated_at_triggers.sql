CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END
$$ language 'plpgsql';

CREATE TRIGGER public_datasets_updated_at
BEFORE UPDATE ON public_datasets
FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER public_dataset_versions_updated_at
BEFORE UPDATE ON public_dataset_versions
FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
