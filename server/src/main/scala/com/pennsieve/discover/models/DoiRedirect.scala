// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

class DoiRedirect(settings: WorkspaceSettings) {
  def getPublisher(): String = settings.publisherName
  private def getUrl(
    templateUrl: String,
    datasetId: Int,
    versionId: Int
  ): String = {
    val replacements = Map(
      "{{datasetId}}" -> datasetId.toString,
      "{{versionId}}" -> versionId.toString
    )
    replacements.foldLeft(templateUrl)((a, b) => a.replace(b._1, b._2))
  }
  def getDatasetUrl(datasetId: Int, versionId: Int): String =
    getUrl(settings.redirectUrl, datasetId, versionId)
  def getReleaseUrl(datasetId: Int, versionId: Int): String =
    getUrl(
      settings.redirectReleaseUrl.getOrElse(settings.redirectUrl),
      datasetId,
      versionId
    )
}

object DoiRedirect {
  def apply(settings: WorkspaceSettings): DoiRedirect =
    new DoiRedirect(settings)
}
