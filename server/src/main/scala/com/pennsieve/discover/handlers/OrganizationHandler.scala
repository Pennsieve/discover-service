// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server.Route
import com.pennsieve.auth.middleware.AkkaDirective.authenticateJwt
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.discover.Authenticator.withOrganizationAccess
import com.pennsieve.discover._
import com.pennsieve.discover.db.PublicDatasetsMapper
import com.pennsieve.discover.logging.logRequestAndResponse
import com.pennsieve.discover.models._
import com.pennsieve.discover.server.definitions.DatasetMetricsDto
import com.pennsieve.discover.server.organization.{
  OrganizationHandler => GuardrailHandler,
  OrganizationResource => GuardrailResource
}

import scala.concurrent.{ ExecutionContext, Future }

/**
  * Handler for public endpoints for getting tags associated with published datasets.
  */
class OrganizationHandler(
  ports: Ports
)(implicit
  executionContext: ExecutionContext
) extends GuardrailHandler {

  override def getOrganizationDatasetMetrics(
    respond: GuardrailResource.GetOrganizationDatasetMetricsResponse.type
  )(
    organizationId: Int
  ): Future[GuardrailResource.GetOrganizationDatasetMetricsResponse] = {
    ports.db
      .run(PublicDatasetsMapper.getOrganizationDatasetMetrics(organizationId))
      .map { organizationDatasets =>
        GuardrailResource.GetOrganizationDatasetMetricsResponse
          .OK(DatasetMetricsDto(organizationDatasets.toVector))
      }
  }
}

object OrganizationHandler {

  def routes(
    ports: Ports
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Route =
    logRequestAndResponse(ports) {
      GuardrailResource.routes(new OrganizationHandler(ports))
    }
}
