CREATE TABLE workspace_settings (
    id SERIAL PRIMARY KEY,
    organization_id INT NOT NULL,
    publisher_name TEXT NOT NULL,
    redirect_url TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(organization_id)
);

CREATE TRIGGER workspace_settings_updated_at
    BEFORE UPDATE ON workspace_settings
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
