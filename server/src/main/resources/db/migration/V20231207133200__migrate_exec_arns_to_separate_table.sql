/*
**  create `publishing_jobs` table to hold execution ARNs
*/
create table discover.publishing_jobs(
  dataset_id int,
  version int,
  job_type varchar(255),
  execution_arn varchar(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

/*
** "migrate" the Publish job ARNs
*/
insert into discover.publishing_jobs
select dataset_id,
       version,
       'PUBLISH',
       execution_arn
from discover.public_dataset_versions
where execution_arn is not null
order by dataset_id, version
;

/*
** "migrate" the Release job ARNs
*/
insert into discover.publishing_jobs
select dataset_id,
       version,
       'RELEASE',
       release_execution_arn
from discover.public_dataset_versions
where release_execution_arn is not null
order by dataset_id, version;

/*
** drop the execution ARN columns from the public_dataset_versions table
*/
alter table discover.public_dataset_versions drop column execution_arn;
alter table discover.public_dataset_versions drop column release_execution_arn;
