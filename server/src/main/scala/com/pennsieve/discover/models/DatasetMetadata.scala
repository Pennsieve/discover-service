// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

object DatasetMetadata {
  private val METADATA_FILE = "metadata.json"
  private val MANIFEST_FILE = "manifest.json"
  private val README_FILE = "readme.md"
  private val CHANGELOG_FILE = "changelog.md"
  private val BANNER = "banner"
  private val README = "readme"

  def metadataFileName(version: PublicDatasetVersion): String =
    version.schemaVersion match {
      case PennsieveSchemaVersion.`1.0` => METADATA_FILE
      case PennsieveSchemaVersion.`2.0` => METADATA_FILE
      case PennsieveSchemaVersion.`3.0` => METADATA_FILE
      case PennsieveSchemaVersion.`4.0` | PennsieveSchemaVersion.`5.0` =>
        MANIFEST_FILE
    }

  /**
    * Assumed locations of items in the publish bucket.
    *
    * TODO: share/communicate these locations from publish job outputs
    */
  def metadataKey(version: PublicDatasetVersion): S3Key.File =
    version.schemaVersion match {
      case PennsieveSchemaVersion.`1.0` => version.s3Key / "metadata.json"
      case PennsieveSchemaVersion.`2.0` => version.s3Key / "metadata.json"
      case PennsieveSchemaVersion.`3.0` => version.s3Key / "metadata.json"
      case PennsieveSchemaVersion.`4.0` | PennsieveSchemaVersion.`5.0` =>
        version.s3Key / MANIFEST_FILE
    }
}
