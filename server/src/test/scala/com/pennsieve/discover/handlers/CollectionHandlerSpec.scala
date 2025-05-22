// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.pennsieve.test.EitherValue._
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.discover.Authenticator.{
  generateServiceToken,
  generateUserToken
}
import com.pennsieve.discover.{ ServiceSpecHarness, TestUtilities }
import com.pennsieve.discover.client.collection.PublishCollectionResponse
import com.pennsieve.discover.client.collection.CollectionClient
import com.pennsieve.discover.client.{ collection, definitions }
import com.pennsieve.discover.client.definitions.{
  DatasetPublishStatus,
  PublishCollectionRequest
}
import com.pennsieve.discover.clients.MockDoiClient
import com.pennsieve.discover.db.{
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper
}
import com.pennsieve.discover.models.{ PublishingWorkflow, S3Bucket, S3Key }
import com.pennsieve.models.DatasetType.Collection
import com.pennsieve.models.PublishStatus.{
  PublishFailed,
  PublishInProgress,
  PublishSucceeded
}
import com.pennsieve.models.{ Degree, License, PublishStatus, RelationshipType }
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CollectionHandlerSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with ScalatestRouteTest
    with ServiceSpecHarness {
  def createRoutes(): Route =
    Route.seal(CollectionHandler.routes(ports))

  def createClient(routes: Route): CollectionClient =
    CollectionClient.httpClient(Route.toFunction(routes))

  private val client = createClient(createRoutes())

  val collectionName = "Dataset"
  val collectionId = 34
  val collectionNodeId = "abc123-xyz-456"
  val ownerId = 1
  val ownerNodeId = "N:user:abc123"
  val ownerFirstName = "Data"
  val ownerLastName = "Digger"
  val ownerOrcid = "0000-0012-3456-7890"

  private val requestBody: PublishCollectionRequest = PublishCollectionRequest(
    name = collectionName,
    description = "This is a test collection for publishing",
    ownerId = ownerId,
    doiCount = 5,
    license = License.`Apache License 2.0`,
    ownerNodeId = ownerNodeId,
    ownerFirstName = ownerFirstName,
    ownerLastName = ownerLastName,
    ownerOrcid = ownerOrcid,
    collectionNodeId = collectionNodeId
  )

  private val customBucketConfig =
    definitions.BucketConfig("org-publish-bucket", "org-embargo-bucket")

  private val customBucketRequestBody =
    requestBody.copy(bucketConfig = Some(customBucketConfig))

  val token: Jwt.Token =
    generateServiceToken(
      ports.jwt,
      organizationId = CollectionHandler.collectionOrgId,
      datasetId = collectionId
    )

  private val authToken = List(Authorization(OAuth2BearerToken(token.value)))

  val userToken: Jwt.Token =
    generateUserToken(
      ports.jwt,
      1,
      CollectionHandler.collectionOrgId,
      Some(collectionId)
    )

  private val userAuthToken = List(
    Authorization(OAuth2BearerToken(userToken.value))
  )

  "POST /collection/{collectionId}/publish" should {
    "fail without a JWT" in {

      val response = client
        .publishCollection(collectionId, requestBody)
        .awaitFinite()
        .value

      response shouldBe PublishCollectionResponse.Unauthorized
    }

    "fail with a user JWT" in {
      val response = client
        .publishCollection(collectionId, requestBody, userAuthToken)
        .awaitFinite()
        .value

      response shouldBe PublishCollectionResponse.Forbidden(
        "Only allowed for service level requests"
      )
    }

    "create a DB entry and link a DOI" in {

      val response = client
        .publishCollection(collectionId, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishCollectionResponse.Created]
        .value

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(
              CollectionHandler.collectionOrgId,
              collectionId
            )
        )
        .awaitFinite()

      publicDataset.name shouldBe requestBody.name
      publicDataset.sourceOrganizationId shouldBe CollectionHandler.collectionOrgId
      publicDataset.sourceDatasetId shouldBe collectionId
      publicDataset.ownerId shouldBe requestBody.ownerId
      publicDataset.ownerFirstName shouldBe requestBody.ownerFirstName
      publicDataset.ownerLastName shouldBe requestBody.ownerLastName
      publicDataset.ownerOrcid shouldBe requestBody.ownerOrcid
      publicDataset.datasetType shouldBe Collection

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      val doiDto = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(CollectionHandler.collectionOrgId, collectionId)
        .get

      publicVersion.version shouldBe 1
      publicVersion.modelCount shouldBe empty
      publicVersion.recordCount shouldBe 0
      publicVersion.fileCount shouldBe 1
      publicVersion.size shouldBe 0
      publicVersion.description shouldBe requestBody.description
      publicVersion.status shouldBe PublishStatus.PublishInProgress
      publicVersion.s3Bucket shouldBe config.s3.publish50Bucket
      publicVersion.s3Key shouldBe S3Key.Version(s"${publicDataset.id}/")
      publicVersion.doi shouldBe doiDto.doi

      response shouldBe com.pennsieve.discover.client.definitions
        .PublishCollectionResponse(
          name = collectionName,
          sourceCollectionId = collectionId,
          publishedDatasetId = publicDataset.id,
          publishedVersion = publicVersion.version,
          status = PublishInProgress,
          lastPublishedDate = Some(publicVersion.createdAt),
          sponsorship = None,
          publicId = publicVersion.doi
        )

    }

    "correctly use custom publish bucket" in {

      client
        .publishCollection(collectionId, customBucketRequestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishCollectionResponse.Created]
        .value

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(
              CollectionHandler.collectionOrgId,
              collectionId
            )
        )
        .awaitFinite()

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      publicVersion.s3Bucket.value shouldBe customBucketConfig.publish

    }

    "return the publishing status of the dataset" in {

      val publicDataset = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = CollectionHandler.collectionOrgId,
        sourceDatasetId = collectionId
      )
      val publicDataset1_V1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset.id,
          status = PublishSucceeded
        )

      val response = client
        .publishCollection(collectionId, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishCollectionResponse.Created]
        .value

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      response shouldBe com.pennsieve.discover.client.definitions
        .PublishCollectionResponse(
          name = collectionName,
          sourceCollectionId = collectionId,
          publishedDatasetId = publicDataset.id,
          publishedVersion = 2,
          status = PublishInProgress,
          lastPublishedDate = Some(publicVersion.createdAt),
          sponsorship = None,
          publicId = publicVersion.doi
        )

      val doiDto = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(CollectionHandler.collectionOrgId, collectionId)
        .get

      publicVersion.version shouldBe 2
      publicVersion.modelCount shouldBe empty
      publicVersion.recordCount shouldBe 0
      publicVersion.fileCount shouldBe 1
      publicVersion.size shouldBe 0
      publicVersion.status shouldBe PublishStatus.PublishInProgress
      publicVersion.s3Bucket shouldBe config.s3.publish50Bucket
      publicVersion.s3Key shouldBe S3Key.Version(s"${publicDataset.id}/")
      publicVersion.doi shouldBe doiDto.doi

    }

    "delete a previously failed version before creating a new one" in {

      val publicDataset = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = CollectionHandler.collectionOrgId,
        sourceDatasetId = collectionId,
        datasetType = Collection
      )
      TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishFailed
      )

      val response = client
        .publishCollection(collectionId, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishCollectionResponse.Created]
        .value

      val latestVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      response shouldBe com.pennsieve.discover.client.definitions
        .PublishCollectionResponse(
          name = collectionName,
          sourceCollectionId = collectionId,
          publishedDatasetId = publicDataset.id,
          publishedVersion = 1,
          status = PublishInProgress,
          lastPublishedDate = Some(latestVersion.createdAt),
          sponsorship = None,
          publicId = latestVersion.doi
        )

      val doiDto = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(CollectionHandler.collectionOrgId, collectionId)
        .get

      latestVersion.version shouldBe 1
      latestVersion.modelCount shouldBe empty
      latestVersion.recordCount shouldBe 0
      latestVersion.fileCount shouldBe 1
      latestVersion.size shouldBe 0
      latestVersion.status shouldBe PublishStatus.PublishInProgress
      latestVersion.s3Bucket shouldBe config.s3.publish50Bucket
      latestVersion.s3Key shouldBe S3Key.Version(s"${publicDataset.id}/")
      latestVersion.doi shouldBe doiDto.doi

    }

    "not create a new DOI if draft DOI is only associated with a failed version" in {

      val publicDataset = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = CollectionHandler.collectionOrgId,
        sourceDatasetId = collectionId,
        datasetType = Collection
      )
      val draftDoi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(CollectionHandler.collectionOrgId, collectionId)
        .doi

      TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishFailed,
        doi = draftDoi
      )

      client
        .publishCollection(collectionId, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishCollectionResponse.Created]
        .value

      val latestVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      latestVersion.doi shouldBe draftDoi

    }

  }
}
