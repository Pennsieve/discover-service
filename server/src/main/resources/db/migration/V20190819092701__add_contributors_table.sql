CREATE TABLE public_contributors (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    orcid VARCHAR(255),
    dataset_id INT NOT NULL,
    version  INT NOT NULL,
    source_contributor_id INT NOT NULL,
    source_user_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER public_contributors_updated_at
BEFORE UPDATE ON public_contributors
FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();