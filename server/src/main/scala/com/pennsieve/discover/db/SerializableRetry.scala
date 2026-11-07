// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import akka.actor.ActorSystem
import akka.pattern.after
import com.pennsieve.discover.db.profile.api._
import org.postgresql.util.PSQLException
import slick.jdbc.TransactionIsolation

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Random

/**
  * Runs an action as a Serializable transaction and runs it again when
  * Postgres cancels it to keep concurrent transactions serializable
  * (SQLSTATE 40001). Concurrent publishes, such as the releases of many
  * repositories at once, otherwise fail with "could not serialize access".
  * The action must be safe to run again: a canceled transaction is rolled
  * back, but side effects it ran (DBIOAction.from) are not.
  */
object SerializableRetry {

  val SerializationFailure = "40001"

  def isSerializationFailure(error: Throwable): Boolean =
    error match {
      case e: PSQLException if e.getSQLState == SerializationFailure => true
      case e if e.getCause != null && (e.getCause ne e) =>
        isSerializationFailure(e.getCause)
      case _ => false
    }

  def run[R](
    db: Database,
    action: DBIOAction[R, NoStream, Effect.All],
    attempts: Int = 5,
    baseDelay: FiniteDuration = 100.millis
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Future[R] = {
    val transaction = action.transactionally
      .withTransactionIsolation(TransactionIsolation.Serializable)

    def attempt(n: Int): Future[R] =
      db.run(transaction).recoverWith {
        case e if n < attempts && isSerializationFailure(e) =>
          // back off with jitter so the conflicting transactions spread out
          val delay = baseDelay * (1L << (n - 1)) +
            Random.nextInt(baseDelay.toMillis.toInt.max(1)).millis
          system.log.warning(
            s"serialization failure, attempt $n of $attempts; retrying in $delay"
          )
          after(delay, system.scheduler)(attempt(n + 1))
      }

    attempt(1)
  }
}
