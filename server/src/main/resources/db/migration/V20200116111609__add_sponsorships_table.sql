CREATE TABLE sponsorships (
    id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL UNIQUE REFERENCES public_datasets(id) ON DELETE CASCADE,
    title VARCHAR(255),
    image_url TEXT,
    markup TEXT
)