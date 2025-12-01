// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.{
  ActorSystemTestKit,
  ServiceSpecHarness,
  TestUtilities
}
import com.pennsieve.discover.models._
import com.pennsieve.test.AwaitableImplicits
import io.circe.syntax._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class RevisionsMapperSpec
    extends AnyWordSpec
    with ServiceSpecHarness
    with AwaitableImplicits
    with Matchers
    with ActorSystemTestKit {

  def run[A](
    dbio: DBIOAction[A, NoStream, Nothing],
    timeout: FiniteDuration = 5.seconds
  ) =
    ports.db.run(dbio).awaitFinite(timeout)

  "RevisionsMapper" should {

    "increment revision numbers" in {
      val version = TestUtilities.createDatasetV1(ports.db)()

      run(RevisionsMapper.create(version)).revision shouldBe 1
      run(RevisionsMapper.create(version)).revision shouldBe 2
      run(RevisionsMapper.create(version)).revision shouldBe 3
    }

    "get the latest revision for a dataset version" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)()
      val v1revision1 = run(RevisionsMapper.create(v1))
      val v1revision2 = run(RevisionsMapper.create(v1))

      val v2 =
        TestUtilities.createNewDatasetVersion(ports.db)(v1.datasetId)
      val v2revision1 = run(RevisionsMapper.create(v2))

      run(RevisionsMapper.getLatestRevision(v2)) shouldBe Some(v2revision1)

      run(RevisionsMapper.getLatestRevisions(List(v1, v2))) shouldBe Map(
        v1 -> Some(v1revision2),
        v2 -> Some(v2revision1)
      )
    }

    "get the latest revision when multiple datasets have the same version" in {
      // Dataset at version 1
      val d1v1 = TestUtilities.createDatasetV1(ports.db)(sourceDatasetId = 1)
      val d1v1revision1 = run(RevisionsMapper.create(d1v1))

      // Different dataset, also at version 1
      val d2v1 = TestUtilities.createDatasetV1(ports.db)(sourceDatasetId = 2)
      val d2v1revision1 = run(RevisionsMapper.create(d2v1))

      run(RevisionsMapper.getLatestRevision(d1v1)) shouldBe Some(d1v1revision1)
      run(RevisionsMapper.getLatestRevision(d2v1)) shouldBe Some(d2v1revision1)

      run(RevisionsMapper.getLatestRevisions(List(d1v1, d2v1))) shouldBe Map(
        d1v1 -> Some(d1v1revision1),
        d2v1 -> Some(d2v1revision1)
      )
    }
  }
}
