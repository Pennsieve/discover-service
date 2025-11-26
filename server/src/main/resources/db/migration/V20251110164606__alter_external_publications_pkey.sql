-- 1. Make relationship_type NOT NULL
ALTER TABLE public_external_publications
    ALTER COLUMN relationship_type SET NOT NULL;

-- 2. Drop the old primary key
ALTER TABLE public_external_publications
    DROP CONSTRAINT public_external_publications_pkey;

-- 3. Add the new primary key
ALTER TABLE public_external_publications
    ADD PRIMARY KEY (doi, dataset_id, version, relationship_type);
