// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import com.pennsieve.discover.models.{
  PublicDataset,
  PublicDatasetVersion,
  S3Bucket,
  S3Key
}

case class DoiCreationException(msg: String) extends Throwable {}

case class DoiServiceException(error: Throwable) extends Throwable {
  override def getMessage: String = error.getMessage
}

case class ForbiddenException(msg: String) extends Throwable {}

case object DuplicateDoiException extends Throwable {}

case class NoDatasetException(id: Int) extends Throwable {
  override def getMessage: String = s"No dataset could be found for id=$id"
}

case class NoDatasetPreviewForUser(userId: Int) extends Throwable {
  override def getMessage: String =
    s"No dataset preview could be found for user=$userId"
}

case object DatasetTooLargeException extends Throwable {}

case object DatasetUnderEmbargo extends Throwable {
  override def getMessage: String =
    s"Dataset is Under Embargo"
}

case class UnknownOrigin(origin: String) extends Throwable {
  override def getMessage: String =
    s"'$origin' is not an allowed value for the DownloadOrigin header"
}

case class MissingParameterException(parameter: String) extends Throwable {
  override def getMessage: String =
    s"Missing parameter '$parameter' from request"
}

case class NoDatasetForSourcesException(
  sourceOrganizationId: Int,
  sourceDatasetId: Int
) extends Throwable {
  override def getMessage: String =
    s"No dataset could be found for sourceOrganization=$sourceOrganizationId, sourceDatasetId=$sourceDatasetId"
}

case class NoDatasetVersionException(id: Int, version: Int) extends Throwable {
  override def getMessage: String =
    s"No dataset could be found for id=$id version=$version"
}

case class NoDatasetForDoiException(doi: String) extends Throwable {
  override def getMessage: String =
    s"No dataset could be found for doi=$doi"
}

case class NoFileVersionException(
  datasetId: Int,
  s3Key: S3Key.File,
  s3Version: String
) extends Throwable {
  override def getMessage: String =
    s"File Version does not exist (datasetId: ${datasetId}) ${s3Key} (version: ${s3Version})"
}

case class DatasetUnpublishedException(
  dataset: PublicDataset,
  version: PublicDatasetVersion
) extends Throwable {
  override def getMessage: String =
    s"Dataset id=${dataset.id} version=${version.version} unpublished"
}

case object NoDoiException extends Throwable {}

case class PublishJobException(msg: String) extends Throwable {}

case object UnauthorizedException extends Throwable {}

case class S3Exception(bucket: S3Bucket, key: S3Key) extends Throwable {
  override def getMessage: String = s"Error streaming s3://$bucket/$key"
}

case class LambdaException(
  s3KeyPrefix: String,
  publishBucket: String,
  embargoBucket: String
) extends Throwable {
  override def getMessage: String =
    s"Failed to run s3clean lambda function for s3_key_prefix=$s3KeyPrefix, publish_bucket=$publishBucket, embargo_bucket=$embargoBucket"
}

case class BadQueryParameter(error: Throwable) extends Throwable {
  override def getMessage: String = error.getMessage
}

case class NoSponsorshipException(sponsorshipId: Int) extends Throwable {
  override def getMessage: String =
    s"No sponsorship could be found for id=$sponsorshipId"
}

case class NoSponsorshipForDatasetException(datasetId: Int) extends Throwable {
  override def getMessage: String =
    s"No sponsorship could be found for dataset=$datasetId"
}

case class NoFileException(id: Int, version: Int, path: S3Key.File)
    extends Throwable {
  override def getMessage: String =
    s"No file could be found for id=$id version=$version path=$path"
}
