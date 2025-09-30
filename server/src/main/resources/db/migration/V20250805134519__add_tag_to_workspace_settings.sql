ALTER TABLE workspace_settings
    ADD COLUMN publisher_tag text;

ALTER TABLE workspace_settings
    DROP CONSTRAINT workspace_settings_organization_id_key;

ALTER TABLE workspace_settings
    ADD CONSTRAINT workspace_settings_organization_id_publisher_tag_key
        UNIQUE NULLS NOT DISTINCT (organization_id, publisher_tag);