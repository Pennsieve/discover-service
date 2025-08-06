// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import com.pennsieve.auth.middleware.Jwt.Role.RoleIdentifier
import com.pennsieve.auth.middleware.{
  DatasetId,
  DatasetPermission,
  Jwt,
  OrganizationId,
  ServiceClaim,
  UserClaim,
  UserId,
  Wildcard
}
import com.pennsieve.auth.middleware.Validator.{
  hasDatasetAccess,
  hasOrganizationAccess,
  isServiceClaim
}
import com.pennsieve.models.Role
import shapeless.syntax.inject._

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }

object Authenticator {

  def generateServiceToken(
    jwt: Jwt.Config,
    organizationId: Int,
    datasetId: Int,
    role: Role = Role.Owner,
    duration: FiniteDuration = 5.minutes
  ): Jwt.Token = {
    val claim = generateServiceClaim(organizationId, datasetId, role, duration)
    Jwt.generateToken(claim)(jwt)
  }

  def generateServiceClaim(
    organizationId: Int,
    datasetId: Int,
    role: Role = Role.Owner,
    duration: FiniteDuration = 5.minutes
  ): Jwt.Claim = {
    val serviceClaim = ServiceClaim(
      List(
        Jwt.OrganizationRole(
          OrganizationId(organizationId).inject[RoleIdentifier[OrganizationId]],
          role
        ),
        Jwt.DatasetRole(
          DatasetId(datasetId).inject[RoleIdentifier[DatasetId]],
          role
        )
      )
    )
    Jwt.generateClaim(serviceClaim, duration)
  }

  def generateUserToken(
    jwt: Jwt.Config,
    userId: Int,
    organizationId: Int,
    datasetId: Option[Int] = None,
    duration: FiniteDuration = 5.minutes
  ): Jwt.Token = {
    val claim = generateUserClaim(userId, organizationId, datasetId, duration)
    Jwt.generateToken(claim)(jwt)
  }

  def generateUserClaim(
    userId: Int,
    organizationId: Int,
    datasetId: Option[Int] = None,
    duration: FiniteDuration = 5.minutes
  ): Jwt.Claim = {
    val organizationRole =
      Jwt.OrganizationRole(
        OrganizationId(organizationId).inject[RoleIdentifier[OrganizationId]],
        Role.Owner
      )

    val datasetRole: Jwt.Role = datasetId match {
      case Some(id) =>
        Jwt.DatasetRole(
          DatasetId(id).inject[RoleIdentifier[DatasetId]],
          Role.Owner
        )
      case None =>
        Jwt.DatasetRole(Wildcard.inject[RoleIdentifier[DatasetId]], Role.Owner)
    }

    val userClaim =
      UserClaim(UserId(userId), List(organizationRole, datasetRole))

    Jwt.generateClaim(userClaim, duration)
  }

  /*
   * Ensure that this claim has access to the given dataset
   */
  def withDatasetAccess[T](
    claim: Jwt.Claim,
    datasetId: Int
  )(
    f: Unit => Future[T]
  )(implicit
    executionContext: ExecutionContext
  ): Future[T] =
    if (hasDatasetAccess(
        claim,
        DatasetId(datasetId),
        DatasetPermission.ViewFiles
      )) f(())
    else
      Future.failed(ForbiddenException(s"Not allowed for dataset $datasetId"))

  def withDatasetOwnerAccess[T](
    claim: Jwt.Claim,
    datasetId: Int
  )(
    f: Unit => Future[T]
  )(implicit
    executionContext: ExecutionContext
  ): Future[T] =
    if (hasDatasetAccess(
        claim,
        DatasetId(datasetId),
        DatasetPermission.RequestCancelPublishRevise
      )) f(())
    else
      Future.failed(ForbiddenException(s"Not allowed for dataset $datasetId"))

  /*
   * Ensure that this claim has access to the given organization
   */
  def withOrganizationAccess[T](
    claim: Jwt.Claim,
    organizationId: Int
  )(
    f: Unit => Future[T]
  )(implicit
    executionContext: ExecutionContext
  ): Future[T] =
    if (hasOrganizationAccess(claim, OrganizationId(organizationId))) f(())
    else
      Future.failed(
        ForbiddenException(s"Not allowed for organization $organizationId")
      )

  /*
   * Ensure that this claim has access to the organization and dataset
   */
  def withAuthorization[T](
    claim: Jwt.Claim,
    organizationId: Int,
    datasetId: Int
  )(
    f: Unit => Future[T]
  )(implicit
    executionContext: ExecutionContext
  ): Future[T] =
    withOrganizationAccess(claim, organizationId) { _ =>
      withDatasetAccess(claim, datasetId) { _ =>
        f(())
      }
    }

  def withServiceOrganizationAuthorization[T](
    claim: Jwt.Claim,
    organizationId: Int
  )(
    f: Unit => Future[T]
  )(implicit
    executionContext: ExecutionContext
  ): Future[T] =
    withOrganizationAccess(claim, organizationId) { _ =>
      if (isServiceClaim(claim)) {
        f(())
      } else
        Future.failed(
          ForbiddenException(s"Only allowed for service level requests")
        )
    }

  def withServiceOwnerAuthorization[T](
    claim: Jwt.Claim,
    organizationId: Int,
    datasetId: Int
  )(
    f: Unit => Future[T]
  )(implicit
    executionContext: ExecutionContext
  ): Future[T] =
    withOrganizationAccess(claim, organizationId) { _ =>
      withDatasetOwnerAccess(claim, datasetId) { _ =>
        if (isServiceClaim(claim)) {
          f(())
        } else
          Future.failed(
            ForbiddenException(s"Only allowed for service level requests")
          )
      }
    }
}
