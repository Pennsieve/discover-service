// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.{ ActorSystemTestKit, ServiceSpecHarness }
import com.pennsieve.test.AwaitableImplicits
import org.postgresql.util.{ PSQLException, PSQLState }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class SerializableRetrySpec
    extends AnyWordSpec
    with ServiceSpecHarness
    with AwaitableImplicits
    with Matchers
    with ActorSystemTestKit {

  def serializationFailure =
    new PSQLException(
      "could not serialize access due to read/write dependencies among transactions",
      PSQLState.SERIALIZATION_FAILURE
    )

  // fails with `error` the first `failures` times it runs
  def flaky(failures: Int, error: => Throwable) = {
    var runs = 0
    val action = DBIO.successful(()).flatMap { _ =>
      runs += 1
      if (runs <= failures) DBIO.failed(error) else DBIO.successful(runs)
    }
    (action, () => runs)
  }

  "SerializableRetry" should {

    "run a transaction again after a serialization failure" in {
      val (action, runs) = flaky(2, serializationFailure)

      SerializableRetry
        .run(ports.db, action, baseDelay = 1.milli)
        .awaitFinite(5.seconds) shouldBe 3
      runs() shouldBe 3
    }

    "give up after the last attempt" in {
      val (action, runs) = flaky(10, serializationFailure)

      val result = SerializableRetry
        .run(ports.db, action, attempts = 3, baseDelay = 1.milli)
        .failed
        .awaitFinite(5.seconds)

      SerializableRetry.isSerializationFailure(result) shouldBe true
      runs() shouldBe 3
    }

    "not run a transaction again after any other failure" in {
      val (action, runs) =
        flaky(1, new PSQLException("boom", PSQLState.UNIQUE_VIOLATION))

      SerializableRetry
        .run(ports.db, action, baseDelay = 1.milli)
        .failed
        .awaitFinite(5.seconds) shouldBe a[PSQLException]
      runs() shouldBe 1
    }

    "find a serialization failure wrapped in another exception" in {
      SerializableRetry.isSerializationFailure(
        new RuntimeException("wrapped", serializationFailure)
      ) shouldBe true
    }
  }
}
