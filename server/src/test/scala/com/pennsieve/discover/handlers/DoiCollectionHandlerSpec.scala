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
import com.pennsieve.discover.client.collection.PublishDoiCollectionResponse
import com.pennsieve.discover.client.collection.FinalizeDoiCollectionResponse
import com.pennsieve.discover.client.collection.CollectionClient
import com.pennsieve.discover.client.definitions.{
  BucketConfig,
  FinalizeDoiCollectionRequest,
  PublishDoiCollectionRequest
}
import com.pennsieve.discover.clients.{ MockDoiClient, MockS3StreamClient }
import com.pennsieve.discover.db.{
  PublicDatasetDoiCollectionDoisMapper,
  PublicDatasetDoiCollectionsMapper,
  PublicDatasetVersionFilesTableMapper,
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper,
  PublicFileVersionsMapper
}
import com.pennsieve.discover.models.{ PublicDatasetDoiCollection, S3Key }
import com.pennsieve.models.DatasetType.Collection
import com.pennsieve.models.PublishStatus.{
  PublishFailed,
  PublishInProgress,
  PublishSucceeded
}
import com.pennsieve.models.{ License, PublishStatus }
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DoiCollectionHandlerSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with ScalatestRouteTest
    with ServiceSpecHarness {
  def createRoutes(): Route =
    Route.seal(DoiCollectionHandler.routes(ports))

  def createClient(routes: Route): CollectionClient =
    CollectionClient.httpClient(Route.toFunction(routes))

  private val client = createClient(createRoutes())

  val collectionName = "Dataset"
  val sourceCollectionId = 34
  val collectionNodeId = "abc123-xyz-456"
  val ownerId = 1
  val ownerNodeId = "N:user:abc123"
  val ownerFirstName = "Data"
  val ownerLastName = "Digger"
  val ownerOrcid = "0000-0012-3456-7890"

  private val pennsieveDoiPrefix = config.doiCollections.pennsieveDoiPrefix

  private val requestBody: PublishDoiCollectionRequest =
    PublishDoiCollectionRequest(
      name = collectionName,
      description = "This is a test collection for publishing",
      banners = Vector(
        "https://example.com/banner_9.png",
        "https://example.com/banner_11.png",
        "https://example.com/banner_31.png",
        "https://example.com/banner_1.png"
      ),
      dois = Vector(s"${pennsieveDoiPrefix}/${TestUtilities.randomString()}"),
      ownerId = ownerId,
      license = License.`Apache License 2.0`,
      ownerNodeId = ownerNodeId,
      ownerFirstName = ownerFirstName,
      ownerLastName = ownerLastName,
      ownerOrcid = ownerOrcid,
      collectionNodeId = collectionNodeId
    )

  private val customBucketConfig =
    BucketConfig("org-publish-bucket", "org-embargo-bucket")

  private val customBucketRequestBody =
    requestBody.copy(bucketConfig = Some(customBucketConfig))

  val token: Jwt.Token =
    generateServiceToken(
      ports.jwt,
      organizationId = DoiCollectionHandler.collectionOrgId,
      datasetId = sourceCollectionId
    )

  private val authToken = List(Authorization(OAuth2BearerToken(token.value)))

  val userToken: Jwt.Token =
    generateUserToken(
      ports.jwt,
      1,
      DoiCollectionHandler.collectionOrgId,
      Some(sourceCollectionId)
    )

  private val userAuthToken = List(
    Authorization(OAuth2BearerToken(userToken.value))
  )

  "POST /collection/{collectionId}/publish" should {
    "fail without a JWT" in {

      val response = client
        .publishDoiCollection(sourceCollectionId, requestBody)
        .awaitFinite()
        .value

      response shouldBe PublishDoiCollectionResponse.Unauthorized
    }

    "fail with a user JWT" in {
      val response = client
        .publishDoiCollection(sourceCollectionId, requestBody, userAuthToken)
        .awaitFinite()
        .value

      response shouldBe PublishDoiCollectionResponse.Forbidden(
        "Only allowed for service level requests"
      )
    }

    "create a DB entry and link a DOI" in {

      val response = client
        .publishDoiCollection(sourceCollectionId, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishDoiCollectionResponse.Created]
        .value

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(
              DoiCollectionHandler.collectionOrgId,
              sourceCollectionId
            )
        )
        .awaitFinite()

      publicDataset.name shouldBe requestBody.name
      publicDataset.sourceOrganizationId shouldBe DoiCollectionHandler.collectionOrgId
      publicDataset.sourceDatasetId shouldBe sourceCollectionId
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
        .getMockDoi(DoiCollectionHandler.collectionOrgId, sourceCollectionId)
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

      val doiCollection = ports.db
        .run(
          PublicDatasetDoiCollectionsMapper
            .getVersion(publicVersion.datasetId, publicVersion.version)
        )
        .awaitFinite()

      doiCollection.banners shouldBe requestBody.banners.toList

      val doiCollectionDOIs = ports.db
        .run(
          PublicDatasetDoiCollectionDoisMapper
            .getDOIs(publicVersion.datasetId, publicVersion.version)
        )
        .awaitFinite()

      doiCollectionDOIs shouldBe requestBody.dois.toList

      response shouldBe com.pennsieve.discover.client.definitions
        .PublishDoiCollectionResponse(
          name = collectionName,
          sourceCollectionId = sourceCollectionId,
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
        .publishDoiCollection(
          sourceCollectionId,
          customBucketRequestBody,
          authToken
        )
        .awaitFinite()
        .value
        .asInstanceOf[PublishDoiCollectionResponse.Created]
        .value

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(
              DoiCollectionHandler.collectionOrgId,
              sourceCollectionId
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

      val publicDataset = TestUtilities.createDoiCollectionDataset(ports.db)(
        sourceDatasetId = sourceCollectionId
      )

      TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishSucceeded
      )

      val response = client
        .publishDoiCollection(sourceCollectionId, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishDoiCollectionResponse.Created]
        .value

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      response shouldBe com.pennsieve.discover.client.definitions
        .PublishDoiCollectionResponse(
          name = collectionName,
          sourceCollectionId = sourceCollectionId,
          publishedDatasetId = publicDataset.id,
          publishedVersion = 2,
          status = PublishInProgress,
          lastPublishedDate = Some(publicVersion.createdAt),
          sponsorship = None,
          publicId = publicVersion.doi
        )

      val doiDto = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(DoiCollectionHandler.collectionOrgId, sourceCollectionId)
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

      val publicDataset = TestUtilities.createDoiCollectionDataset(ports.db)(
        sourceDatasetId = sourceCollectionId
      )
      TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishFailed
      )

      val response = client
        .publishDoiCollection(sourceCollectionId, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishDoiCollectionResponse.Created]
        .value

      val latestVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      response shouldBe com.pennsieve.discover.client.definitions
        .PublishDoiCollectionResponse(
          name = collectionName,
          sourceCollectionId = sourceCollectionId,
          publishedDatasetId = publicDataset.id,
          publishedVersion = 1,
          status = PublishInProgress,
          lastPublishedDate = Some(latestVersion.createdAt),
          sponsorship = None,
          publicId = latestVersion.doi
        )

      val doiDto = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(DoiCollectionHandler.collectionOrgId, sourceCollectionId)
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

      val publicDataset = TestUtilities.createDoiCollectionDataset(ports.db)(
        sourceDatasetId = sourceCollectionId
      )
      val draftDoi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(DoiCollectionHandler.collectionOrgId, sourceCollectionId)
        .doi

      TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishFailed,
        doi = draftDoi
      )

      client
        .publishDoiCollection(sourceCollectionId, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishDoiCollectionResponse.Created]
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

    "fail with Bad Request if given non-Pennsieve DOI" in {
      val nonPennsieveDoi = s"10.99999/${TestUtilities.randomString()}"
      val nonPennsieveBody = requestBody.copy(
        dois = Vector(
          s"$pennsieveDoiPrefix/${TestUtilities.randomString()}",
          nonPennsieveDoi
        )
      )
      val response = client
        .publishDoiCollection(sourceCollectionId, nonPennsieveBody, authToken)
        .awaitFinite()
        .value

      response shouldBe PublishDoiCollectionResponse.BadRequest(
        s"Collection contains non-Pennsieve DOIs: $nonPennsieveDoi"
      )
    }

    "fail with Bad Request if given no DOIs" in {
      val noDoisBody = requestBody.copy(dois = Vector.empty)
      val response = client
        .publishDoiCollection(sourceCollectionId, noDoisBody, authToken)
        .awaitFinite()
        .value

      response shouldBe PublishDoiCollectionResponse.BadRequest(
        "no DOIs in request"
      )
    }

    "fail with Bad Request if given unpublished Pennsieve DOI" in {
      // In anticipation of allowing non-Pennsieve DOIs, the validation code does not
      // currently complain about DOIs that
      // are not found in our DB at all, so no need to create a test dataset to go with
      // this DOI.
      val publishedDoi = s"$pennsieveDoiPrefix/${TestUtilities.randomString()}"

      val unpublishedDataset = TestUtilities.createDataset(ports.db)()

      val unpublishedDoi =
        s"$pennsieveDoiPrefix/${TestUtilities.randomString()}"

      val unpublishedVersion = TestUtilities.createNewDatasetVersion(ports.db)(
        id = unpublishedDataset.id,
        status = PublishStatus.Unpublished,
        doi = unpublishedDoi
      )

      val unpublishedBody =
        requestBody.copy(dois = Vector(unpublishedDoi, publishedDoi))
      val response = client
        .publishDoiCollection(sourceCollectionId, unpublishedBody, authToken)
        .awaitFinite()
        .value

      response shouldBe PublishDoiCollectionResponse.BadRequest(
        s"Collection contains unpublished DOIs: ($unpublishedDoi,${unpublishedVersion.status})"
      )
    }

    "fail with Bad Request if given a Pennsieve DOI that is itself a DOI Collection" in {
      val publishedDoi = s"$pennsieveDoiPrefix/${TestUtilities.randomString()}"

      val doiCollectionDataset = TestUtilities.createDoiCollectionDataset(
        ports.db
      )(sourceDatasetId = sourceCollectionId)

      val doiCollectionDoi =
        s"$pennsieveDoiPrefix/${TestUtilities.randomString()}"

      TestUtilities.createNewDatasetVersion(ports.db)(
        id = doiCollectionDataset.id,
        status = PublishStatus.PublishSucceeded,
        doi = doiCollectionDoi
      )

      val collectionInACollectionBody =
        requestBody.copy(dois = Vector(doiCollectionDoi, publishedDoi))
      val response = client
        .publishDoiCollection(
          sourceCollectionId,
          collectionInACollectionBody,
          authToken
        )
        .awaitFinite()
        .value

      response shouldBe PublishDoiCollectionResponse.BadRequest(
        s"Collection contains collection DOIs: $doiCollectionDoi"
      )
    }

  }
  "POST /collection/{collectionId}/finalize" should {

    val randomRequestBody: FinalizeDoiCollectionRequest =
      FinalizeDoiCollectionRequest(
        publishedDatasetId = 11,
        publishedVersion = 1,
        publishSuccess = true,
        fileCount = 1,
        totalSize = 17,
        manifestKey = TestUtilities.randomString(),
        manifestVersionId = TestUtilities.randomString()
      )

    "fail without a JWT" in {

      val response = client
        .finalizeDoiCollection(sourceCollectionId, randomRequestBody)
        .awaitFinite()
        .value

      response shouldBe FinalizeDoiCollectionResponse.Unauthorized
    }

    "fail with a user JWT" in {
      val response = client
        .finalizeDoiCollection(
          sourceCollectionId,
          randomRequestBody,
          userAuthToken
        )
        .awaitFinite()
        .value

      response shouldBe FinalizeDoiCollectionResponse.Forbidden(
        "Only allowed for service level requests"
      )
    }

    "handle failure notifications" in {
      client
        .publishDoiCollection(sourceCollectionId, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishDoiCollectionResponse.Created]
        .value

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(
              DoiCollectionHandler.collectionOrgId,
              sourceCollectionId
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

      publicVersion.status shouldBe PublishStatus.PublishInProgress

      val finalizeRequest = FinalizeDoiCollectionRequest(
        publishedDatasetId = publicDataset.id,
        publishedVersion = publicVersion.version,
        publishSuccess = false,
        fileCount = 0,
        totalSize = 0,
        manifestKey = "",
        manifestVersionId = ""
      )

      val finalizeResponse = client
        .finalizeDoiCollection(sourceCollectionId, finalizeRequest, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[FinalizeDoiCollectionResponse.OK]
        .value

      finalizeResponse.status shouldBe PublishStatus.PublishFailed

      val updatedVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      updatedVersion.status shouldBe PublishStatus.PublishFailed
    }

    "handle a successful publish" in {
      client
        .publishDoiCollection(sourceCollectionId, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishDoiCollectionResponse.Created]
        .value

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(
              DoiCollectionHandler.collectionOrgId,
              sourceCollectionId
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

      publicVersion.status shouldBe PublishStatus.PublishInProgress

      val manifestJson =
        s"""
           |{
           |  "pennsieveDatasetId": 1791,
           |  "version": 14,
           |  "name": "f1df25c0-f5b7-43a2-8482-250acaabfc18",
           |  "description": "29ceef0e-bd65-45b7-a5e6-8feb8da813ea",
           |  "creator": {
           |    "first_name": "9ea5bebc-620a-4019-b0d9-fbe42bcb24b5",
           |    "last_name": "13fa3d78-2bbc-4ac0-90ba-051907689dc8",
           |    "orcid": "3510ff82-91ac-4883-a120-0d394d609ab6",
           |    "middle_initial": "b",
           |    "degree": "Ph.D."
           |  },
           |  "contributors": [
           |    {
           |      "first_name": "9ea5bebc-620a-4019-b0d9-fbe42bcb24b5",
           |      "last_name": "13fa3d78-2bbc-4ac0-90ba-051907689dc8",
           |      "orcid": "3510ff82-91ac-4883-a120-0d394d609ab6",
           |      "middle_initial": "b",
           |      "degree": "Ph.D."
           |    }
           |  ],
           |  "sourceOrganization": "",
           |  "keywords": [
           |    "f272254c-da93-4b8b-aba6-8aef77f10457",
           |    "b880141d-6f8f-4441-aa1b-b4a126de19ae"
           |  ],
           |  "datePublished": "2025-07-07",
           |  "license": "Apache 2.0",
           |  "@id": "10.1111/4b5a87a7-82ca-4122-aa16-697135cd14bb",
           |  "publisher": "The University of Pennsylvania",
           |  "@context": "http://schema.org/",
           |  "@type": "Collection",
           |  "schemaVersion": "http://schema.org/version/3.7/",
           |  "files": [
           |    {
           |      "name": "manifest.json",
           |      "path": "manifest.json",
           |      "size": 1445,
           |      "fileType": "Json"
           |    }
           |  ],
           |  "references": {"ids": [
           |    "10.1111/635e620f-ca11-469b-88de-f9ad84557d45",
           |    "10.1111/b5833ade-a29a-4d09-a040-a2b31ea54587",
           |    "10.1111/a3222e95-e4d6-416c-9610-fac73180bbbf"
           |  ]},
           |  "pennsieveSchemaVersion": "5.0"
           |}
           |""".stripMargin
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .storeDatasetMetadata(publicVersion, manifestJson)

      val expectedFileCount = 1
      val expectedTotalSize = 1445
      val expectedManifestKey = s"${publicDataset.id}/manifest.json"
      val expectedManifestVersionId = TestUtilities.randomString()

      val finalizeRequest = FinalizeDoiCollectionRequest(
        publishedDatasetId = publicDataset.id,
        publishedVersion = publicVersion.version,
        publishSuccess = true,
        fileCount = expectedFileCount,
        totalSize = expectedTotalSize,
        manifestKey = expectedManifestKey,
        manifestVersionId = expectedManifestVersionId
      )

      val finalizeResponse = client
        .finalizeDoiCollection(sourceCollectionId, finalizeRequest, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[FinalizeDoiCollectionResponse.OK]
        .value

      finalizeResponse.status shouldBe PublishStatus.PublishSucceeded

      // check: dataset, version, files
      val publicDatasetFinal = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(
              PublicDatasetDoiCollection.collectionOrgId,
              sourceCollectionId
            )
        )
        .awaitFinite()

      val publicVersionFinal = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDatasetFinal.id)
        )
        .awaitFinite()
        .get

      publicVersionFinal.status shouldBe PublishStatus.PublishSucceeded
      publicVersionFinal.fileCount shouldBe expectedFileCount
      publicVersionFinal.size shouldBe expectedTotalSize

      val links = ports.db
        .run(
          PublicDatasetVersionFilesTableMapper
            .getLinks(publicDatasetFinal.id, publicVersionFinal.version)
        )
        .awaitFinite()

      links.length shouldEqual 1

      val files = ports.db
        .run(PublicFileVersionsMapper.getAll(publicDatasetFinal.id))
        .awaitFinite()

      files.length shouldEqual 1
      val actualManifestFile = files.head

      actualManifestFile.s3Key.value shouldBe expectedManifestKey
      actualManifestFile.s3Version shouldBe expectedManifestVersionId
    }

  }
}
