// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.scripts

import akka.{ Done, NotUsed }
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import akka.actor.ActorSystem
import akka.stream.alpakka.s3.scaladsl.S3
import cats.data._
import cats.implicits._
import com.pennsieve.discover._
import com.pennsieve.discover.db._
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models._
import com.pennsieve.models.PublishStatus
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }

/**
  * One-off script to ingest dataset file data from S3 and add it to Postgres.
  */
object ImportFiles extends StrictLogging {

  def main(args: Array[String]): Unit = {

    val dryRun: Boolean = args.headOption match {
      case Some("false") => false
      case _ => true
    }

    if (dryRun) println("*** DRY RUN ***")

    val config: Config = Config.load

    implicit val system: ActorSystem = ActorSystem("discover-service")
    implicit val executionContext: ExecutionContext = system.dispatcher

    implicit val ports: Ports = Ports(config)

    try {
      val query = PublicDatasetVersionsMapper
        .filter(_.status === (PublishStatus.PublishSucceeded: PublishStatus))
        .join(PublicDatasetsMapper)
        .on(_.datasetId === _.id)
        .result
        .map(_.toList)
        .map(Source(_))

      val end = Source
        .futureSource(ports.db.run(query))
        .mapAsync(1) {
          case (version, dataset) => {
            println(s"Migrating dataset ${dataset.id}/${version.version}")
            (for {
              metadata <- ports.s3StreamClient
                .readDatasetMetadata(version)

              _ = println(s"Found ${metadata.files.length} files")

              _ <- if (!dryRun) {
                val query = for {
                  _ <- PublicFilesMapper
                    .forVersion(version)
                    .delete
                  _ <- PublicFilesMapper.createMany(version, metadata.files)
                  _ <- PublicDatasetVersionsMapper
                    .filter(_.datasetId === dataset.id)
                    .filter(_.version === version.version)
                    .map(_.fileCount)
                    .update(metadata.files.length)
                } yield ()
                ports.db.run(query.transactionally)
              } else Future.successful(())

            } yield ()).recover {
              case e: S3Exception => {
                println(
                  s"Cannot migrate ${version.datasetId}/${version.version}",
                  e
                )
              }
            }
          }
        }
        .runWith(Sink.ignore)

      Await.result(end, 10.minutes)
      logger.info("Done")

    } finally {
      logger.info("Shutting down actor system...")
      ports.db.close()
      system.terminate()
      Await.result(system.terminate(), 5.seconds)
    }
  }
}
