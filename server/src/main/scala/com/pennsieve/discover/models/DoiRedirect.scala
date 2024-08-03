// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

class DoiRedirect(settings: WorkspaceSettings) {
  def getPublisher(): String = settings.publisherName
  def getUrl(datasetId: Int, versionId: Int): String = {
    val replacements = Map(
      "{{datasetId}}" -> datasetId.toString,
      "{{versionId}}" -> versionId.toString
    )
    replacements.foldLeft(settings.redirectUrl)((a, b) => a.replace(b._1, b._2))
  }
}

object DoiRedirect {
  def apply(settings: WorkspaceSettings): DoiRedirect =
    new DoiRedirect(settings)
}
