// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import com.pennsieve.discover.models.{ PublicDataset, PublicDatasetVersion }

object DoiRedirect {
  def getUrl(
    publicDiscoverUrl: String,
    publicDataset: PublicDataset,
    publicVersion: PublicDatasetVersion
  ): String = {
    if (publicDataset.sourceOrganizationName == "SPARC Consortium" &&
      publicDiscoverUrl == "https://disover.pennsieve.io") {
      getSPARCUrl(publicDataset.id, publicVersion.version)
    } else {
      getDiscoverUrl(publicDiscoverUrl, publicDataset.id, publicVersion.version)
    }
  }

  def getPublisher(publicDataset: PublicDataset): String = {
    if (publicDataset.sourceOrganizationName == "SPARC Consortium") {
      "SPARC Consortium"
    } else {
      "Pennsieve Discover"
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
