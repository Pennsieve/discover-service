// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover

import com.blackfynn.discover.Authenticator.{
  generateServiceClaim,
  generateUserClaim,
  withAuthorization,
  withServiceOwnerAuthorization
}
import com.blackfynn.models.Role
import com.blackfynn.test.AwaitableImplicits
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.{ ExecutionContext, Future }
import ExecutionContext.Implicits.global

class AuthenticatorSpec
    extends WordSpec
    with Matchers
    with AwaitableImplicits
    with ScalaFutures {

  val userId = 1
  val organizationId = 1
  val datasetId = 2

  "withAuthorization" should {

    "allow a JWT with organization and dataset access" in {
      val claim = generateServiceClaim(organizationId, datasetId)
      val result = withAuthorization(claim, organizationId, datasetId) { _ =>
        Future.successful(1)
      }
      result.awaitFinite() shouldBe 1
    }

    "reject a JWT without organization access" in {
      val claim = generateServiceClaim(10, datasetId)
      val result = withAuthorization(claim, organizationId, datasetId) { _ =>
        Future.successful(1)
      }
      whenReady(result.failed)(e => e shouldBe an[ForbiddenException])
    }

    "reject a JWT without dataset access" in {
      val claim = generateServiceClaim(organizationId, 20)
      val result = withAuthorization(claim, organizationId, datasetId) { _ =>
        Future.successful(1)
      }
      whenReady(result.failed)(e => e shouldBe an[ForbiddenException])
    }
  }

  "withServiceOwnerAuthorization" should {

    "allow a service JWT with organization and dataset owner access" in {
      val claim = generateServiceClaim(organizationId, datasetId)
      val result =
        withServiceOwnerAuthorization(claim, organizationId, datasetId) { _ =>
          Future.successful(1)
        }
      result.awaitFinite() shouldBe 1
    }

    "reject a service JWT without owner access to the dataset" in {
      val claim = generateServiceClaim(organizationId, datasetId, Role.Viewer)
      val result =
        withServiceOwnerAuthorization(claim, organizationId, datasetId) { _ =>
          Future.successful(1)
        }
      whenReady(result.failed)(e => e shouldBe an[ForbiddenException])
    }

    "reject a JWT with owner access to the dataset that is NOT a service claim" in {
      val claim = generateUserClaim(userId, organizationId, Some(datasetId))
      val result =
        withServiceOwnerAuthorization(claim, organizationId, datasetId) { _ =>
          Future.successful(1)
        }
      whenReady(result.failed)(e => e shouldBe an[ForbiddenException])
    }

    "reject a JWT without organization access" in {
      val claim = generateServiceClaim(10, datasetId)
      val result =
        withServiceOwnerAuthorization(claim, organizationId, datasetId) { _ =>
          Future.successful(1)
        }
      whenReady(result.failed)(e => e shouldBe an[ForbiddenException])
    }

    "reject a JWT without dataset access" in {
      val claim = generateServiceClaim(organizationId, 20)
      val result =
        withServiceOwnerAuthorization(claim, organizationId, datasetId) { _ =>
          Future.successful(1)
        }
      whenReady(result.failed)(e => e shouldBe an[ForbiddenException])
    }
  }

}
