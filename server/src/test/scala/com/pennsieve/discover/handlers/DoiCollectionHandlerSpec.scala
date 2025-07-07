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
           |  "pennsieveDatasetId": 3862,
           |  "version": 3,
           |  "name": "90acb367-515d-43b3-b88c-54403cbdc9b6",
           |  "description": "c302d2ec-7ae6-457b-8a69-4715cac9c8aa",
           |  "creator": {
           |    "first_name": "81db83d6-ad29-43d6-97c4-500396355b38",
           |    "last_name": "9eedf149-f13f-4c55-a3d5-a274060a58f1",
           |    "orcid": "d54200dc-009d-4ca8-b277-ff0d5274ddd3",
           |    "middle_initial": "0",
           |    "degree": "M.S."
           |  },
           |  "contributors": [
           |    {
           |      "first_name": "81db83d6-ad29-43d6-97c4-500396355b38",
           |      "last_name": "9eedf149-f13f-4c55-a3d5-a274060a58f1",
           |      "orcid": "d54200dc-009d-4ca8-b277-ff0d5274ddd3",
           |      "middle_initial": "0",
           |      "degree": "M.S."
           |    }
           |  ],
           |  "sourceOrganization": "",
           |  "keywords": [
           |    "813228ad-bd2f-42b7-acf9-70b95cb2c144",
           |    "9d79aabf-21c2-4870-ae75-8def7d6218cf"
           |  ],
           |  "datePublished": "2025-07-07",
           |  "license": "BSD 3-Clause \\"New\\" or \\"Revised\\" License",
           |  "@id": "10.1111/05a5a504-a0b1-4023-b788-041228f01c41",
           |  "publisher": "The University of Pennsylvania",
           |  "@context": "http://schema.org/",
           |  "@type": "Collection",
           |  "schemaVersion": "http://schema.org/version/3.7/",
           |  "files": [
           |    {
           |      "name": "manifest.json",
           |      "path": "manifest.json",
           |      "size": 1478,
           |      "fileType": "Json"
           |    }
           |  ],
           |  "references": {
           |    "ids": [
           |      "10.1111/31080ffa-858b-40f1-96b0-d042c4a67928",
           |      "10.1111/116bd2df-0be3-4c80-9bd0-5cdb9210a414",
           |      "10.1111/5d2ec43a-45d6-4d96-a841-371f7a30e10e"
           |    ]
           |  },
           |  "pennsieveSchemaVersion": "5.0"
           |}
           |""".stripMargin
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .storeDatasetMetadata(publicVersion, manifestJson)

      val expectedFileCount = 1
      val expectedTotalSize = 1478
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
