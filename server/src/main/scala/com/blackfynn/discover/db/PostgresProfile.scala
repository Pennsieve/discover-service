// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.db

import com.blackfynn.models.{ Degree, License, PublishStatus, RelationshipType }
import com.blackfynn.discover.models.{
  DownloadOrigin,
  ObjectVersion,
  PennsieveSchemaVersion,
  S3Bucket,
  S3Key
}
import com.github.tminglei.slickpg._
import slick.ast.BaseTypedType
import slick.jdbc.{ JdbcCapabilities, JdbcType }
import slick.basic.Capability

trait PostgresProfile
    extends ExPostgresProfile
    with PgArraySupport
    with PgDate2Support
    with PgHStoreSupport
    with PgCirceJsonSupport
    with PgLTreeSupport {

  override protected def computeCapabilities: Set[Capability] =
    super.computeCapabilities + JdbcCapabilities.insertOrUpdate

  trait Implicits { self: API with HStoreImplicits =>

    implicit val countHashMapMapper
      : JdbcType[Map[String, Long]] with BaseTypedType[Map[String, Long]] =
      MappedColumnType.base[Map[String, Long], Map[String, String]](
        counts => counts.mapValues(_.toString),
        hstore => hstore.mapValues(_.toLong)
      )

    implicit val publishStatusMapper
      : JdbcType[PublishStatus] with BaseTypedType[PublishStatus] =
      MappedColumnType.base[PublishStatus, String](
        s => s.entryName,
        s => PublishStatus.withName(s)
      )

    implicit val relationshipTypeMapper
      : JdbcType[RelationshipType] with BaseTypedType[RelationshipType] =
      MappedColumnType.base[RelationshipType, String](
        s => s.entryName,
        s => RelationshipType.withName(s)
      )

    implicit val degreeMapper: JdbcType[Degree] with BaseTypedType[Degree] =
      MappedColumnType
        .base[Degree, String](s => s.entryName, s => Degree.withName(s))

    implicit val downloadOriginMapper
      : JdbcType[DownloadOrigin] with BaseTypedType[DownloadOrigin] =
      MappedColumnType
        .base[DownloadOrigin, String](
          s => s.entryName,
          s => DownloadOrigin.withName(s)
        )

    implicit val licenseMapper: JdbcType[License] with BaseTypedType[License] =
      MappedColumnType
        .base[License, String](s => s.entryName, s => License.withName(s))

    implicit val PennsieveSchemaVersionMapper: JdbcType[PennsieveSchemaVersion]
      with BaseTypedType[PennsieveSchemaVersion] =
      MappedColumnType
        .base[PennsieveSchemaVersion, String](
          s => s.entryName,
          s => PennsieveSchemaVersion.withName(s)
        )

    implicit val s3BucketMapper
      : JdbcType[S3Bucket] with BaseTypedType[S3Bucket] =
      MappedColumnType
        .base[S3Bucket, String](s => s.value, s => S3Bucket(s))

    implicit val s3FileKeyMapper
      : JdbcType[S3Key.File] with BaseTypedType[S3Key.File] =
      MappedColumnType
        .base[S3Key.File, String](s => s.value, s => S3Key.File(s))

    implicit val s3ObjectVersionKeyMapper
      : JdbcType[ObjectVersion] with BaseTypedType[ObjectVersion] =
      MappedColumnType
        .base[ObjectVersion, String](s => s.value, s => ObjectVersion(s))

    implicit val s3DatasetKeyMapper
      : JdbcType[S3Key.Dataset] with BaseTypedType[S3Key.Dataset] =
      MappedColumnType
        .base[S3Key.Dataset, String](s => s.value, s => S3Key.Dataset(s))

  }

  override val pgjson = "jsonb" // jsonb support is in postgres 9.4.0 onward; for 9.3.x use "json"

  object PostgresAPI
      extends API
      with CirceImplicits
      with DateTimeImplicits
      with ArrayImplicits
      with HStoreImplicits
      with LTreeImplicits
      with Implicits
      with SimpleArrayPlainImplicits

  override val api = PostgresAPI
}
