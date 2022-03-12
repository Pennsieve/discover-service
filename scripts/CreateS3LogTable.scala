import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.Http
import akka.stream.Materializer

import cats.data.EitherT
import cats.implicits._

import com.pennsieve.models.License._
import com.pennsieve.models.License
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.service.utilities.SingleHttpResponder
import com.pennsieve.discover._
import scalikejdbc._
import scalikejdbc.athena._

import io.circe.Json
import io.circe.syntax._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

sealed trait CoreError extends Exception

object CreateS3LogTable extends App {

  implicit val system: ActorSystem = ActorSystem("discover-service-scripts")
  implicit val executionContext: ExecutionContext = system.dispatcher

  val config: com.pennsieve.discover.Config = com.pennsieve.discover.Config.load

  if (args.length != 1) {
    println("usage:")
    println("CreateS3LogTable dryRun")
  } else {
    val dryRun = Try(args(0).toBoolean).getOrElse(true)

    DB.athena { implicit s =>
      val s3PublishLogsBucket = config.s3.publishLogsBucket.toString
      val AccessLogspath = config.s3.accessLogsPath.toString
      val s3AccessLogsLocation = "s3://"+s3PublishLogsBucket+"/"+AccessLogspath
      val r =
        sql"""CREATE EXTERNAL TABLE `s3_access_logs_db.discover`(
             |  `bucketowner` STRING,
             |  `bucket_name` STRING,
             |  `requestdatetime` STRING,
             |  `remoteip` STRING,
             |  `requester` STRING,
             |  `requestid` STRING,
             |  `operation` STRING,
             |  `key` STRING,
             |  `request_uri` STRING,
             |  `httpstatus` STRING,
             |  `errorcode` STRING,
             |  `bytessent` BIGINT,
             |  `objectsize` BIGINT,
             |  `totaltime` STRING,
             |  `turnaroundtime` STRING,
             |  `referrer` STRING,
             |  `useragent` STRING,
             |  `versionid` STRING,
             |  `hostid` STRING,
             |  `sigv` STRING,
             |  `ciphersuite` STRING,
             |  `authtype` STRING,
             |  `endpoint` STRING,
             |  `tlsversion` STRING)
             |ROW FORMAT SERDE
             |  'org.apache.hadoop.hive.serde2.RegexSerDe'
             |WITH SERDEPROPERTIES (
             |  'input.regex'='([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$$')
             |STORED AS INPUTFORMAT
             |  'org.apache.hadoop.mapred.TextInputFormat'
             |OUTPUTFORMAT
             |  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
             |LOCATION
             |  $s3AccessLogsLocation
             |""".stripMargin
      if (dryRun) {
        println(r)
      } else {
        r.execute().apply()
      }
    }
    println("Done")
  }
}
