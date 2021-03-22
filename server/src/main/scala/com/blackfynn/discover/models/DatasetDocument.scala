// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import com.blackfynn.discover.Config
import com.blackfynn.discover.server.definitions
import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

/**
  * The entire PublicDatasetDTO is embedded in the dataset index so that we can
  * return the entire DatasetsPage response without having to join the Elastic
  * response with data in Postgres.
  */
final case class DatasetDocument(
  dataset: definitions.PublicDatasetDTO,
  readme: Readme,
  contributors: Option[Seq[String]],
  // This is a hack to work around an issue with Elastic4s. See
  // `SearchClient.datasetMapping` for a full explanation.
  //
  // TODO: This is optional to prevent deserialization errors while deploying
  // this new code. Make this non-optional once this has been released to
  // production.
  rawName: Option[String]
) {
  def id: String = dataset.id.toString
}

object DatasetDocument {
  implicit val encoder: Encoder[DatasetDocument] =
    deriveEncoder[DatasetDocument]
  implicit val decoder: Decoder[DatasetDocument] =
    deriveDecoder[DatasetDocument]

  def apply(
    dataset: definitions.PublicDatasetDTO,
    readme: Readme
  ): DatasetDocument =
    DatasetDocument(
      dataset,
      readme,
      Some(
        s"${dataset.ownerFirstName} ${dataset.ownerLastName} ${dataset.ownerOrcid}" +:
          dataset.contributors.map(
            contributor =>
              s"""${contributor.firstName} ${contributor.lastName}${contributor.orcid
                .map(id => s" $id")
                .getOrElse("")}"""
          )
      ),
      Some(dataset.name.toLowerCase)
    )

  def apply(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    contributors: List[PublicContributor],
    readme: Readme,
    revision: Option[Revision] = None,
    sponsorship: Option[Sponsorship] = None,
    collections: List[PublicCollection],
    externalPublications: List[PublicExternalPublication]
  )(implicit
    config: Config
  ): DatasetDocument =
    DatasetDocument(
      PublicDatasetDTO(
        dataset,
        version,
        contributors,
        sponsorship,
        revision,
        collections,
        externalPublications,
        None
      ),
      readme
    )
}
