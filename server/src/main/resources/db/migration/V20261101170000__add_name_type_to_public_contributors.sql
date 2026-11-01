/*
** A contributor may be an organisation (a lab, a university, a project) rather
** than a person: name_type 'Organizational', named by first_name whole. NULL is
** a person, as every contributor so far. Code releases take their authors from
** the repository's CITATION.cff, where an author is often an organisation;
** DataCite gets them as Organizational creators.
*/
ALTER TABLE public_contributors ADD COLUMN IF NOT EXISTS name_type VARCHAR(32);
