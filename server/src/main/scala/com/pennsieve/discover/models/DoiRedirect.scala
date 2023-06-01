// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

object DoiRedirect {
  def getUrl(
    publicDiscoverUrl: String,
    publicDataset: PublicDataset,
    publicVersion: PublicDatasetVersion
  ): String = {
    if ((publicDataset.sourceOrganizationName == "SPARC" || publicDataset.sourceOrganizationName == "SPARC Consortium") &&
      publicDiscoverUrl == "https://discover.pennsieve.io") {
      getSPARCUrl(publicDataset.id, publicVersion.version)
    } else if (publicDataset.sourceOrganizationName == "RE-JOIN" && publicDiscoverUrl == "https://discover.pennsieve.io") {
      getSPARCUrl(publicDataset.id, publicVersion.version)
    } else {
      getDiscoverUrl(publicDiscoverUrl, publicDataset.id, publicVersion.version)
    }
  }

  def getPublisher(publicDataset: PublicDataset): Option[String] = {
    if ((publicDataset.sourceOrganizationName == "SPARC" || publicDataset.sourceOrganizationName == "SPARC Consortium")) {
      Some("SPARC Consortium")
    } else if (publicDataset.sourceOrganizationName == "RE-JOIN") {
      Some("RE-JOIN Consortium")
    } else {
      Some("Pennsieve Discover")
    }
  }

  def getSPARCUrl(datasetId: Int, version: Int): String = {
    s"https://sparc.science/datasets/${datasetId}/version/${version}"
  }

  def getDiscoverUrl(
    publicDiscoverUrl: String,
    datasetId: Int,
    version: Int
  ): String = {
    s"${publicDiscoverUrl}/datasets/${datasetId}/version/${version}"
  }
}
