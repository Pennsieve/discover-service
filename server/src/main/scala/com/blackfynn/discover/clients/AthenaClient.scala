// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.clients

import com.blackfynn.discover.models.{ DatasetDownload, DownloadOrigin }
import scalikejdbc._
import scalikejdbc.athena._
import java.time.LocalDate
import scala.concurrent.{ ExecutionContext, Future }

trait AthenaClient {

  def getDatasetDownloadsForRange(
    startDate: LocalDate,
    endDate: LocalDate
  )(implicit
    ec: ExecutionContext
  ): List[DatasetDownload]
}

class AthenaClientImpl() extends AthenaClient {

  def getDatasetDownloadsForRange(
    startDate: LocalDate,
    endDate: LocalDate
  )(implicit
    ec: ExecutionContext
  ): List[DatasetDownload] = {
    println(startDate)
    println(endDate)
    DB.athena { implicit s =>
      //The discover table in the s3_access_logs_db is a column-for-column ingestion of the s3 access logs
      //An analysis of the logs showed that each full dataset download results in a row in the logs that always
      // shows operation = 'REST.GET.BUCKET'. The url column of such rows also contain the dataset_id and version
      // passed as the prefix when it is a full dataset download. We take advantage of this in two places: in the
      // select block to extract the dataset_id and version, but also in the WHERE block to reject other
      // 'REST.GET.BUCKET' operations which are not dataset downloads and do not contain a dataset_id.
      // Likewise, we also remove rows where the requester column contains 's3clean' as those are linked to
      // unpublish actions.

      sql"""SELECT regexp_extract(request_uri,'.*prefix=(\d+)(?:%2F|\/)(\d+)(:?%2F|\/).*', 1) AS dataset_id,
           |     regexp_extract(request_uri, '.*prefix=(\d+)(?:%2F|\/)(\d+)(:?%2F|\/).*', 2) AS version,
           |     date_parse(requestdatetime, '%d/%b/%Y:%H:%i:%S +%f') as dl_date,
           |     requestid
           |FROM s3_access_logs_db.discover
           |WHERE operation = 'REST.GET.BUCKET'
           |        AND requester NOT LIKE '%s3clean%'
           |        AND regexp_extract(request_uri, '.*prefix=(\d+)(:?%2F|\/)(\d+)(:?%2F|\/).*', 1) is NOT null
           |        AND date_parse(requestdatetime, '%d/%b/%Y:%H:%i:%S +%f') >= date_parse(${startDate.toString}, '%Y-%m-%d')
           |        AND date_parse(requestdatetime, '%d/%b/%Y:%H:%i:%S +%f') < date_parse(${endDate.toString}, '%Y-%m-%d')
           |ORDER BY  dataset_id, version, dl_date;""".stripMargin
        .map(
          rs =>
            DatasetDownload(
              rs.int("dataset_id"),
              rs.int("version"),
              Some(DownloadOrigin.AWSRequesterPayer),
              Some(rs.string("requestid")),
              rs.offsetDateTime("dl_date")
            )
        )
        .list
        .apply()
    }
  }

}
