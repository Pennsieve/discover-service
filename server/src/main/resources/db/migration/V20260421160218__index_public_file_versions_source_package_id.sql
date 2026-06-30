/*
** Add an index on public_file_versions.source_package_id.
**
** Why: the GET /packages/{sourcePackageId}/files endpoint
** (FileHandler.getFileFromSourcePackageId, backed by
** PublicFileVersionsTable.getFileFromSourcePackageId) filters a
** 4-way join on source_package_id. That column has had no index
** since the table was introduced in V20230704074854, so every call
** does a sequential scan. Observed response time: ~13s on prod.
**
** Partial on IS NOT NULL: source_package_id is nullable and many
** historical rows have NULL. The lookup predicate "= 'N:package:...'"
** can never match NULL, so excluding NULL rows keeps the index small
** and cheap to maintain. Postgres proves the implication and will
** still use the index for the lookup.
**
** Non-concurrent: Flyway 4.x wraps migrations in a transaction and
** CREATE INDEX CONCURRENTLY cannot run in a transaction. Takes an
** AccessExclusiveLock during build, briefly blocking writes. Writes
** to public_file_versions happen only during dataset publishing —
** acceptable transient blockage.
*/
CREATE INDEX IF NOT EXISTS public_file_versions_source_package_id_idx
    ON public_file_versions (source_package_id)
    WHERE source_package_id IS NOT NULL;