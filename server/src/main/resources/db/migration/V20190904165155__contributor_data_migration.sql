--this create statement is only here to pass tests since it was created by a manual csv import in discover db
create table if not exists contrib_export (
  first_name varchar(255),
  last_name varchar(255),
  orcid varchar(19),
  user_id INTEGER,
  contributor_id INTEGER,
  dataset_id INTEGER,
  organization_id INTEGER);

insert into public_contributors (first_name, last_name, orcid, dataset_id, version, source_contributor_id, source_user_id)
with contrib (
    full_name,
    dataset_id,
    version,
    source_organization_id,
    source_dataset_id
    )
as (
    select distinct
        trim(regexp_replace(unnest(contributors), '\s+', ' ', 'g')) as full_name,
        dataset_id,
        version,
        source_organization_id,
        source_dataset_id
    from public_dataset_versions pdv
    join public_datasets pd
        on pdv.dataset_id = pd.id
    ),
contribs as(
select
    case when position(',' in full_name)>0 THEN
            trim(coalesce(substring(replace(full_name, ',', '') from ' .*$'),''))
        ELSE
            trim(substring(full_name from '^[^\s]*'))
        END as first_name,
        case when position(',' in full_name)>0 THEN
            trim(substring(replace(full_name, ',', '') from '^[^\s]*'))
        ELSE
            trim(coalesce(substring(full_name from ' .*$'),''))
        END as last_name,

    dataset_id,
    version,
    source_organization_id,
    source_dataset_id
    from contrib)
select
  c.first_name,
  c.last_name,
  ce.orcid,
  c.dataset_id,
  c.version,
  ce.contributor_id as source_contributor_id,
  ce.user_id as source_user_id
from contribs c join contrib_export ce on c.source_organization_id = ce.organization_id and c.source_dataset_id = ce.dataset_id and c.first_name = ce.first_name and c.last_name=ce.last_name;
