// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import com.pennsieve.discover.Config
import com.pennsieve.discover.clients.DatasetPreview
import com.pennsieve.discover.server.definitions
import com.pennsieve.discover.utils.joinPath
import io.scalaland.chimney.dsl._

object PublicDatasetDTO {

  def apply(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    contributors: IndexedSeq[definitions.PublicContributorDto],
    sponsorship: Option[definitions.SponsorshipDto],
    revision: Option[Revision],
    collections: Option[IndexedSeq[definitions.PublicCollectionDto]],
    externalPublications: Option[
      IndexedSeq[definitions.PublicExternalPublicationDto]
    ],
    datasetPreview: Option[DatasetPreview]
  )(implicit
    config: Config
  ): definitions.PublicDatasetDto =
    version
      .into[definitions.PublicDatasetDto]
      .withFieldComputed(_.id, _ => dataset.id)
      .withFieldComputed(_.sourceDatasetId, _ => Some(dataset.sourceDatasetId))
      .withFieldComputed(_.name, _ => dataset.name)
      .withFieldComputed(_.ownerId, _ => Some(dataset.ownerId))
      .withFieldComputed(_.ownerFirstName, _ => dataset.ownerFirstName)
      .withFieldComputed(_.ownerLastName, _ => dataset.ownerLastName)
      .withFieldComputed(_.ownerOrcid, _ => dataset.ownerOrcid)
      .withFieldComputed(
        _.organizationName,
        _ => dataset.sourceOrganizationName
      )
      .withFieldComputed(
        _.organizationId,
        _ => Some(dataset.sourceOrganizationId)
      )
      .withFieldComputed(_.license, _ => dataset.license)
      .withFieldComputed(_.tags, _ => dataset.tags.toVector)
      .withFieldComputed(
        _.modelCount,
        _ =>
          version.modelCount.toVector
            .map { case (k, v) => definitions.ModelCount(k, v) }
      )
      .withFieldComputed(_.embargo, _ => Some(version.underEmbargo))
      .withFieldComputed(_.embargoReleaseDate, _ => version.embargoReleaseDate)
      .withFieldComputed(
        _.embargoAccess,
        _ => datasetPreview.map((p: DatasetPreview) => p.embargoAccess)
      )
      .withFieldComputed(_.uri, _ => version.uri)
      .withFieldComputed(_.arn, _ => version.arn)
      .withFieldComputed(
        _.banner,
        _ =>
          version.banner.map(
            key =>
              joinPath(
                config.assetsUrl,
                config.s3.assetsKeyPrefix,
                key.toString
              )
          )
      )
      .withFieldComputed(_.contributors, _ => contributors.toVector)
      .withFieldComputed(_.collections, _ => collections.map(_.toVector))
      .withFieldComputed(
        _.externalPublications,
        _ => externalPublications.map(_.toVector)
      )
      .withFieldComputed(
        _.readme,
        _ =>
          version.readme.map(
            key =>
              joinPath(
                config.assetsUrl,
                config.s3.assetsKeyPrefix,
                key.toString
              )
          )
      )
      .withFieldComputed(
        _.changelog,
        _ =>
          version.changelog.map(
            key =>
              joinPath(
                config.assetsUrl,
                config.s3.assetsKeyPrefix,
                key.toString
              )
          )
      )
      .withFieldComputed(_.sponsorship, _ => sponsorship)
      // TODO: pennsieveSchemaVersion can be non-optional once ElasticSearch has
      // been reindexed in production so that all datasets have a schema
      // version.
      .withFieldComputed(
        _.pennsieveSchemaVersion,
        _ => Some(version.schemaVersion.toString)
      )
      .withFieldComputed(_.revision, _ => revision.map(_.revision))

      // TODO: firstPublishedAt and versionPublishedAt can be non-optional once
      // ElasticSearch has been reindexed in production.
      .withFieldComputed(_.firstPublishedAt, _ => Some(dataset.createdAt))
      .withFieldComputed(_.versionPublishedAt, _ => Some(version.createdAt))
      .withFieldComputed(_.revisedAt, _ => revision.map(_.createdAt))
      .withFieldComputed(_.datasetType, _ => dataset.datasetType.entryName)
      .transform

  def apply(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    contributors: Seq[PublicContributor],
    sponsorship: Option[Sponsorship] = None,
    revision: Option[Revision] = None,
    collections: Seq[PublicCollection],
    externalPublications: Seq[PublicExternalPublication],
    datasetPreview: Option[DatasetPreview]
  )(implicit
    config: Config
  ): definitions.PublicDatasetDto = {
    apply(
      dataset = dataset,
      version = version,
      contributors =
        contributors.map(PublicContributorDTO.apply(_)).toIndexedSeq,
      sponsorship = sponsorship.map(_.toDTO),
      revision = revision,
      collections =
        Some(collections.map(PublicCollectionDTO.apply(_)).toIndexedSeq),
      externalPublications = Some(
        externalPublications
          .map(
            p =>
              definitions
                .PublicExternalPublicationDto(p.doi, p.relationshipType)
          )
          .toIndexedSeq
      ),
      datasetPreview = datasetPreview
    )
  }
}
