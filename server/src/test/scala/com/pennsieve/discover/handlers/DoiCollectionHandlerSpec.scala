// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.circe.parser.decode
import com.pennsieve.test.EitherValue._
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.discover.Authenticator.{
  generateServiceToken,
  generateUserToken
}
import com.pennsieve.discover.{ Ports, ServiceSpecHarness, TestUtilities }
import com.pennsieve.discover.client.collection.PublishDoiCollectionResponse
import com.pennsieve.discover.client.collection.FinalizeDoiCollectionResponse
import com.pennsieve.discover.client.collection.CollectionClient
import com.pennsieve.discover.client.collection.UnpublishDoiCollectionResponse.{
  Forbidden,
  NoContent,
  OK,
  Unauthorized
}
import com.pennsieve.discover.client.definitions
import com.pennsieve.discover.client.definitions.{
  BucketConfig,
  DatasetPublishStatus,
  FinalizeDoiCollectionRequest,
  InternalContributor,
  PublishDoiCollectionRequest
}
import com.pennsieve.discover.clients.{
  LambdaRequest,
  MockDoiClient,
  MockLambdaClient,
  MockS3StreamClient,
  MockSearchClient,
  MockSqsAsyncClient
}
import com.pennsieve.discover.db.{
  PublicContributorsMapper,
  PublicDatasetDoiCollectionDoisMapper,
  PublicDatasetDoiCollectionsMapper,
  PublicDatasetVersionFilesTableMapper,
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper,
  PublicExternalPublicationsMapper,
  PublicFileVersionsMapper
}
import com.pennsieve.discover.models.{
  PublicDatasetVersion,
  PublishingWorkflow,
  S3CleanupStage,
  S3Key
}
import com.pennsieve.models.DatasetType.Collection
import com.pennsieve.models.PublishStatus.{
  PublishFailed,
  PublishInProgress,
  PublishSucceeded,
  Unpublished
}
import com.pennsieve.models.{ License, PublishStatus }
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.notifications.PushDoiRequest
import com.pennsieve.discover.notifications.SQSNotificationType.PUSH_DOI
import com.pennsieve.models.RelationshipType.References
import org.scalatest.Inspectors.forAll

class DoiCollectionHandlerSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with ScalatestRouteTest
    with ServiceSpecHarness {

  val collectionName = "Dataset"
  val sourceCollectionId = 34
  val collectionNodeId = "abc123-xyz-456"
  val ownerId = 1
  val ownerNodeId = "N:user:abc123"
  val ownerFirstName = "Data"
  val ownerLastName = "Digger"
  val ownerOrcid = "0000-0012-3456-7890"

  val internalContributor =
    new InternalContributor(
      id = 3,
      firstName = ownerFirstName,
      lastName = ownerLastName,
      orcid = Some(ownerOrcid),
      userId = Some(ownerId)
    )

  private val customBucketConfig =
    BucketConfig("org-publish-bucket", "org-embargo-bucket")

  private var client: CollectionClient = _
  private var pennsieveDoiPrefix: String = _
  private var collectionOrgId: Int = _
  private var requestBody: PublishDoiCollectionRequest = _
  private var customBucketRequestBody: PublishDoiCollectionRequest = _
  private var token: Jwt.Token = _
  private var authToken: List[Authorization] = _
  private var userToken: Jwt.Token = _
  private var userAuthToken: List[Authorization] = _

  def createRoutes(): Route =
    Route.seal(DoiCollectionHandler.routes(ports))

  def createClient(routes: Route): CollectionClient =
    CollectionClient.httpClient(Route.toFunction(routes))

  override def afterStart(): Unit = {
    super.afterStart()

    // we're overriding ServiceSpecHarness ports with a mock SQS client
    ports = ports.copy(sqsClient = new MockSqsAsyncClient())
    client = createClient(createRoutes())

    pennsieveDoiPrefix = config.doiCollections.pennsieveDoiPrefix
    collectionOrgId = config.doiCollections.idSpace.id

    requestBody = PublishDoiCollectionRequest(
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
      collectionNodeId = collectionNodeId,
      contributors = Vector(internalContributor)
    )

    customBucketRequestBody =
      requestBody.copy(bucketConfig = Some(customBucketConfig))

    token = generateServiceToken(
      ports.jwt,
      organizationId = collectionOrgId,
      datasetId = sourceCollectionId
    )

    authToken = List(Authorization(OAuth2BearerToken(token.value)))

    userToken =
      generateUserToken(ports.jwt, 1, collectionOrgId, Some(sourceCollectionId))

    userAuthToken = List(Authorization(OAuth2BearerToken(userToken.value)))
  }

  override def afterEach(): Unit = {
    super.afterEach()

    ports.sqsClient
      .asInstanceOf[MockSqsAsyncClient]
      .sendMessageCalls
      .clear()
  }

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
            .getDatasetFromSourceIds(collectionOrgId, sourceCollectionId)
        )
        .awaitFinite()

      publicDataset.name shouldBe requestBody.name
      publicDataset.sourceOrganizationId shouldBe collectionOrgId
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
        .getMockDoi(collectionOrgId, sourceCollectionId)
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

      val contributors = ports.db
        .run(
          PublicContributorsMapper
            .getContributorsByDatasetAndVersion(publicDataset, publicVersion)
        )
        .awaitFinite()

      TestUtilities.internalAndPublicContributorsMatch(
        publicVersion.datasetId,
        publicVersion.version,
        requestBody.contributors.toList,
        contributors
      )

      val externalPubs = ports.db
        .run(
          PublicExternalPublicationsMapper
            .getByDatasetAndVersion(publicDataset, publicVersion)
        )
        .awaitFinite()

      externalPubs should have length requestBody.dois.length
      val externalPubsByDoi = externalPubs.map(p => p.doi -> p).toMap
      forAll(requestBody.dois) { doi =>
        externalPubsByDoi should contain key doi
        val externalPub = externalPubsByDoi(doi)

        externalPub.relationshipType shouldBe References
        externalPub.datasetId shouldBe publicDataset.id
        externalPub.version shouldBe publicVersion.version
      }

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
            .getDatasetFromSourceIds(collectionOrgId, sourceCollectionId)
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

      val publicDataset = TestUtilities.createDoiCollectionDataset(
        ports.db,
        ports.config.doiCollections.idSpace
      )(sourceDatasetId = sourceCollectionId)

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
        .getMockDoi(collectionOrgId, sourceCollectionId)
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

      val publicDataset = TestUtilities.createDoiCollectionDataset(
        ports.db,
        ports.config.doiCollections.idSpace
      )(sourceDatasetId = sourceCollectionId)
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
        .getMockDoi(collectionOrgId, sourceCollectionId)
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

      val publicDataset = TestUtilities.createDoiCollectionDataset(
        ports.db,
        ports.config.doiCollections.idSpace
      )(sourceDatasetId = sourceCollectionId)
      val draftDoi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(collectionOrgId, sourceCollectionId)
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

      val unpublishedVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
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
        ports.db,
        ports.config.doiCollections.idSpace
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
            .getDatasetFromSourceIds(collectionOrgId, sourceCollectionId)
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

      val (publishResponse, finalizeRequest, finalizeResponse) =
        publishDoiCollection()

      finalizeResponse.status shouldBe PublishStatus.PublishSucceeded

      val sqsMessages = ports.sqsClient
        .asInstanceOf[MockSqsAsyncClient]
        .sendMessageCalls

      sqsMessages should have size 1

      val message = decode[PushDoiRequest](sqsMessages.head.messageBody())

      message.value shouldBe PushDoiRequest(
        jobType = PUSH_DOI,
        datasetId = publishResponse.publishedDatasetId,
        version = publishResponse.publishedVersion,
        doi = publishResponse.publicId
      )

      // check: dataset, version, files
      val publicDatasetFinal = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(collectionOrgId, sourceCollectionId)
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
      publicVersionFinal.fileCount shouldBe finalizeRequest.fileCount
      publicVersionFinal.size shouldBe finalizeRequest.totalSize

      val files = ports.db
        .run(PublicFileVersionsMapper.getAll(publicDatasetFinal.id))
        .awaitFinite()

      files.length shouldEqual 1
      val actualManifestFile = files.head

      actualManifestFile.s3Key.value shouldBe finalizeRequest.manifestKey
      actualManifestFile.s3Version shouldBe finalizeRequest.manifestVersionId
      actualManifestFile.fileType shouldBe "Json"

      val links = ports.db
        .run(
          PublicDatasetVersionFilesTableMapper
            .getLinks(publicDatasetFinal.id, publicVersionFinal.version)
        )
        .awaitFinite()

      links.length shouldEqual 1
      links.head.fileId shouldBe actualManifestFile.id
    }

    "fail if called on a version that is in a failed state" in {

      val publicDataset = TestUtilities.createDoiCollectionDataset(
        ports.db,
        ports.config.doiCollections.idSpace
      )(sourceDatasetId = sourceCollectionId)
      val beforeVersion = TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishFailed
      )

      val finalizeRequest = FinalizeDoiCollectionRequest(
        publishedDatasetId = publicDataset.id,
        publishedVersion = beforeVersion.version,
        publishSuccess = true,
        fileCount = 1,
        totalSize = 1234,
        manifestKey = s"${publicDataset.id}/manifest.json",
        manifestVersionId = TestUtilities.randomString()
      )

      val finalizeResponse = client
        .finalizeDoiCollection(sourceCollectionId, finalizeRequest, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[FinalizeDoiCollectionResponse.BadRequest]
        .value

      finalizeResponse shouldBe s"no DOICollection publish in progress; status is ${beforeVersion.status}"

      val afterVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      afterVersion.version shouldBe beforeVersion.version
      afterVersion.status shouldBe beforeVersion.status
    }

  }

  "POST /collection/{datasetId}/unpublish" should {

    "fail without a JWT" in {

      val response = client
        .unpublishDoiCollection(collectionId = sourceCollectionId)
        .awaitFinite()
        .value

      response shouldBe Unauthorized
    }

    "fail with a user JWT" in {
      val response = client
        .unpublishDoiCollection(sourceCollectionId, userAuthToken)
        .awaitFinite()
        .value

      response shouldBe Forbidden("Only allowed for service level requests")
    }

    "unpublish a dataset" in {

      val (publishResponse, finalizeRequest, finalizeResponse) =
        publishDoiCollection()

      val response =
        client
          .unpublishDoiCollection(sourceCollectionId, authToken)
          .awaitFinite()
          .value
          .asInstanceOf[OK]
          .value

      response.status shouldBe Unpublished

      val version = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publishResponse.publishedDatasetId)
        )
        .await
        .value

      version.status shouldBe Unpublished

      // Note: the S3 Clean Lambda is invoked at the end of Publish to "tidy," and at Unpublish time
      ports.lambdaClient
        .asInstanceOf[MockLambdaClient]
        .requests should contain atLeastOneElementOf List(
        LambdaRequest(
          publishResponse.publishedDatasetId.toString,
          publishResponse.publishedDatasetId,
          None,
          version.s3Bucket.value,
          version.s3Bucket.value,
          S3CleanupStage.Unpublish,
          true
        )
      )

      ports.searchClient
        .asInstanceOf[MockSearchClient]
        .indexedDatasets shouldBe empty
    }

    "rollback a failed version when unpublishing" in {

      val dataset = TestUtilities.createDoiCollectionDataset(
        ports.db,
        ports.config.doiCollections.idSpace
      )(name = collectionName, sourceDatasetId = sourceCollectionId)

      val version = TestUtilities.createNewDatasetVersion(ports.db)(
        id = dataset.id,
        status = PublishFailed
      )

      val doiCollection =
        TestUtilities.createDatasetDoiCollection(ports.db)(
          datasetId = version.datasetId,
          datasetVersion = version.version,
          banners = TestUtilities.randomBannerUrls
        )

      TestUtilities.addDoiCollectionDois(ports.db)(
        doiCollection = doiCollection,
        dois =
          List(TestUtilities.randomPennsieveDoi(ports.config.doiCollections))
      )

      val response = client
        .unpublishDoiCollection(sourceCollectionId, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[OK]
        .value

      response shouldBe DatasetPublishStatus(
        collectionName,
        collectionOrgId,
        sourceCollectionId,
        None,
        0,
        PublishStatus.NotPublished,
        None,
        workflowId = PublishingWorkflow.Version4
      )

      ports.db
        .run(
          PublicDatasetVersionsMapper
            .filter(_.datasetId === version.datasetId)
            .result
        )
        .await shouldBe empty

      ports.db
        .run(
          PublicDatasetDoiCollectionsMapper
            .filter(_.datasetId === version.datasetId)
            .result
        )
        .await shouldBe empty

      ports.db
        .run(
          PublicDatasetDoiCollectionDoisMapper
            .filter(_.datasetId === version.datasetId)
            .result
        )
        .await shouldBe empty

      ports.lambdaClient
        .asInstanceOf[MockLambdaClient]
        .requests shouldBe empty

    }

    "fail to unpublish a dataset that is currently publishing" in {

      val dataset = TestUtilities.createDoiCollectionDataset(
        ports.db,
        ports.config.doiCollections.idSpace
      )(name = collectionName, sourceDatasetId = sourceCollectionId)

      val version = TestUtilities.createNewDatasetVersion(ports.db)(
        id = dataset.id,
        status = PublishInProgress
      )

      val response = client
        .unpublishDoiCollection(sourceCollectionId, authToken)
        .awaitFinite()
        .value

      response shouldBe Forbidden(
        "Cannot unpublish a DOI Collection that is being published"
      )
    }

    "respond with NoContent for a dataset that was never published" in {

      val response =
        client
          .unpublishDoiCollection(sourceCollectionId, authToken)
          .awaitFinite()
          .value

      response shouldBe NoContent
    }

  }

  private def publishDoiCollection(): (
    definitions.PublishDoiCollectionResponse,
    FinalizeDoiCollectionRequest,
    definitions.FinalizeDoiCollectionResponse
  ) = {
    val publishResponse = client
      .publishDoiCollection(sourceCollectionId, requestBody, authToken)
      .awaitFinite()
      .value
      .asInstanceOf[PublishDoiCollectionResponse.Created]
      .value

    val publicDataset = ports.db
      .run(
        PublicDatasetsMapper
          .getDatasetFromSourceIds(collectionOrgId, sourceCollectionId)
      )
      .awaitFinite()

    val version: PublicDatasetVersion = ports.db
      .run(
        PublicDatasetVersionsMapper
          .getLatestVersion(publicDataset.id)
      )
      .awaitFinite()
      .get

    val manifestJson =
      s"""
         |{
         |  "pennsieveDatasetId": ${sourceCollectionId},
         |  "version": ${version.version},
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
         |  "sourceOrganization": "Discover Collections",
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
      .storeDatasetMetadata(version, manifestJson)

    val finalizeRequest = FinalizeDoiCollectionRequest(
      publishedDatasetId = publicDataset.id,
      publishedVersion = version.version,
      publishSuccess = true,
      fileCount = 1,
      totalSize = 1234,
      manifestKey = s"${publicDataset.id}/manifest.json",
      manifestVersionId = TestUtilities.randomString()
    )
    val finalizeResponse = client
      .finalizeDoiCollection(sourceCollectionId, finalizeRequest, authToken)
      .awaitFinite()
      .value
      .asInstanceOf[FinalizeDoiCollectionResponse.OK]
      .value

    (publishResponse, finalizeRequest, finalizeResponse)
  }

}
