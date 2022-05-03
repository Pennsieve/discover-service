ThisBuild / organization := "com.pennsieve"


lazy val scala212 = "2.12.11"
lazy val scala213 = "2.13.8"
lazy val supportedScalaVersions = List(scala212, scala213)

ThisBuild / scalaVersion := scala212

ThisBuild / scalacOptions ++= Seq(
  "-encoding", "utf-8",
  "-deprecation",
  "-explaintypes",
  "-feature",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-Ypartial-unification",
  "-Ywarn-infer-any",
)

// Run tests in a separate JVM to prevent resource leaks.
ThisBuild / Test / fork := true

ThisBuild / resolvers ++= Seq(
  "Pennsieve Releases" at "https://nexus.pennsieve.cc/repository/maven-releases",
  "Pennsieve Snapshots" at "https://nexus.pennsieve.cc/repository/maven-snapshots",
)

ThisBuild / credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "nexus.pennsieve.cc",
  sys.env("PENNSIEVE_NEXUS_USER"),
  sys.env("PENNSIEVE_NEXUS_PW")
)

// Until https://github.com/coursier/coursier/issues/1815 is fixed
ThisBuild / useCoursier := false

ThisBuild / version := sys.props.get("version").getOrElse("SNAPSHOT")

lazy val headerLicenseValue = Some(HeaderLicense.Custom(
  "Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved."
))
lazy val headerMappingsValue = HeaderFileType.scala -> HeaderCommentStyle.cppStyleLineComment

// Dependency hell is brewing here:
//
// Elastic4s cannot be upgraded because it will bump Circe to 12.0
// Circe cannot be upgraded because pennsieve-api depends on the generated client, and is currently on 11.0
// AWS SDK cannot be upgraded because it bumps Jackson > 10.0, which breaks Elastic4s
//
// ...help.
//
// The dependencyOverrides below fix issues with Jackson for AWS and Elastic4s

lazy val akkaHttpVersion         = "10.1.11"
lazy val akkaVersion             = "2.6.5"
lazy val alpakkaVersion          = "1.1.0"
lazy val circeVersion            = "0.11.0"
lazy val coreVersion             = "166-27f7fae"
lazy val authMiddlewareVersion   = "5.1.3"
lazy val serviceUtilitiesVersion = "8-9751ee3"
lazy val utilitiesVersion        = "4-55953e4"
lazy val doiServiceClientVersion = "12-756107b"
lazy val slickVersion            = "3.2.3"
lazy val slickPgVersion          = "0.16.3"
lazy val dockerItVersion         = "0.9.7"
lazy val logbackVersion          = "1.2.3"
lazy val awsSdkVersion           = "2.10.56"
lazy val pureConfigVersion       = "0.10.2"
lazy val elastic4sVersion        = "6.5.1"
lazy val catsVersion             = "2.0.0"
lazy val jacksonVersion          = "2.9.6"

lazy val Integration = config("integration") extend(Test)

lazy val server = project
  .enablePlugins(AutomateHeaderPlugin)
  .enablePlugins(DockerPlugin)
  .configs(Integration)
  .settings(
    name := "discover-service",
    headerLicense := headerLicenseValue,
    headerMappings := headerMappings.value + headerMappingsValue,
    scalafmtOnCompile := true,
    assembly / test := {},
    Test / fork := true,
    Test / testForkedParallel := true,
    // Only run integration tests with the `integration:test` command
    inConfig(Integration)(Defaults.testTasks),
    Test / testOptions := Seq(Tests.Filter(! _.toLowerCase.contains("integration"))),
    Integration / testOptions := Seq(Tests.Filter(_.toLowerCase.contains("integration"))),

    Compile / mainClass := Some("com.pennsieve.discover.Server"),
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
    ),
    libraryDependencies ++= Seq(
      "com.pennsieve" %% "service-utilities" % serviceUtilitiesVersion,
      "com.pennsieve" %% "utilities" % utilitiesVersion,
      "com.pennsieve" %% "auth-middleware" % authMiddlewareVersion,
      "com.pennsieve" %% "core-models" % coreVersion,
      "com.pennsieve" %% "doi-service-client" % doiServiceClientVersion,

      "org.typelevel" %% "cats-core" % catsVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,

      "commons-io" % "commons-io" % "2.6",
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-xml" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.lightbend.akka" %% "akka-stream-alpakka-sqs" % alpakkaVersion,
      "com.lightbend.akka" %% "akka-stream-alpakka-s3" % alpakkaVersion,
      "com.lightbend.akka" %% "akka-stream-alpakka-awslambda" % alpakkaVersion,
      "com.lightbend.akka" %% "akka-stream-alpakka-csv" % alpakkaVersion,

      "software.amazon.awssdk" % "lambda" % awsSdkVersion,
      "software.amazon.awssdk" % "sfn" % awsSdkVersion,
      "software.amazon.awssdk" % "sns" % awsSdkVersion,
      "software.amazon.awssdk" % "s3" % awsSdkVersion,

      "com.github.pureconfig" %% "pureconfig" % pureConfigVersion,
      "com.github.pureconfig" %% "pureconfig-squants" % pureConfigVersion,

      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",

      "com.typesafe.slick" %% "slick" % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp" % slickVersion,
      "com.github.tminglei" %% "slick-pg" % slickPgVersion,
      "com.github.tminglei" %% "slick-pg_circe-json" % slickPgVersion,
      "org.postgresql" % "postgresql" % "42.2.4",
      "com.zaxxer" % "HikariCP" % "3.3.1",

      "io.scalaland" %% "chimney" % "0.2.1",

      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "ch.qos.logback" % "logback-core" % logbackVersion,
      "net.logstash.logback" % "logstash-logback-encoder" % "5.2",

      "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
      "com.sksamuel.elastic4s" %% "elastic4s-http" % elastic4sVersion,
      "com.sksamuel.elastic4s" %% "elastic4s-circe" % elastic4sVersion,

      "org.apache.commons" % "commons-lang3" % "3.9",
      "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",

      "org.scalikejdbc" %% "scalikejdbc" % "3.4.0",
      "com.zaneli" %% "scalikejdbc-athena" % "0.2.4",

      // Test dependencies
      "org.scalatest" %% "scalatest"% "3.0.5" % Test,
      "com.whisk" %% "docker-testkit-scalatest" % dockerItVersion % Test,
      "com.whisk" %% "docker-testkit-impl-spotify" % dockerItVersion % Test,
      "com.pennsieve" %% "utilities" % utilitiesVersion % "test" classifier "tests",
      "com.pennsieve" %% "service-utilities" % serviceUtilitiesVersion % "test" classifier "tests",
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
      "org.apache.httpcomponents" % "httpclient" % "4.5.8" % Test,
      "software.amazon.awssdk" % "s3" % awsSdkVersion % Test,
      "com.sksamuel.elastic4s" %% "elastic4s-testkit" % elastic4sVersion % Test,
    ),
    Compile / guardrailTasks := List(
      ScalaServer(file("openapi/discover-service.yml"), pkg="com.pennsieve.discover.server"),
      ScalaServer(file("openapi/discover-service-internal.yml"), pkg="com.pennsieve.discover.server")
    ),
    docker / dockerfile := {
      val artifact: File = assembly.value
      val artifactTargetPath = s"/app/${artifact.name}"
      new Dockerfile {
        from("pennsieve/java-cloudwrap:10-jre-slim-0.5.9")
        copy(artifact, artifactTargetPath, chown="pennsieve:pennsieve")
        copy(baseDirectory.value / "bin" / "run.sh", "/app/run.sh", chown="pennsieve:pennsieve")
        run("mkdir", "-p", "/home/pennsieve/.postgresql")
        run("wget", "-qO", "/home/pennsieve/.postgresql/root.crt", "https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem")
        run("wget", "-qO", "/app/newrelic.jar", "http://download.newrelic.com/newrelic/java-agent/newrelic-agent/current/newrelic.jar")
        run("wget", "-qO", "/app/newrelic.yml", "http://download.newrelic.com/newrelic/java-agent/newrelic-agent/current/newrelic.yml")
        cmd("--service", "discover-service", "exec", "app/run.sh", artifactTargetPath)
      }
    },
    docker / imageNames := Seq(
      ImageName("pennsieve/discover-service:latest")
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @_*) => MergeStrategy.discard
      case PathList(ps @ _*) if ps.last endsWith "AWSCredentials.class"  => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith "AWSCredentialsProvider.class"  => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith "AWSSessionCredentials.class"  => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith "AWSSessionCredentialsProvider.class"  => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith "public-suffix-list.txt"  => MergeStrategy.first
      case PathList("codegen-resources", "service-2.json", _ @_*) => MergeStrategy.discard
      case PathList("codegen-resources", "paginators-1.json", _ @_*) => MergeStrategy.discard
      case PathList("codegen-resources", "customization.config", _ @_*) => MergeStrategy.discard
      case PathList("codegen-resources", "examples-1.json", _ @_*) => MergeStrategy.discard
      case PathList("codegen-resources", "waiters-2.json", _ @_*) => MergeStrategy.discard

      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    coverageExcludedPackages :=
      """
       | com.pennsieve.discover.client\..*;
       | com.pennsieve.discover.server\..*;
       | com.pennsieve.discover.Server;
      """.stripMargin.replace("\n", ""),
    coverageMinimum := 70,
    coverageFailOnMinimum := true,
  )
  .dependsOn(client % "test->compile")


lazy val syncElasticSearch = project
  .enablePlugins(AutomateHeaderPlugin)
  .enablePlugins(DockerPlugin)
  .settings(
    name := "discover-service-sync-elasticsearch",
    headerLicense := headerLicenseValue,
    headerMappings := headerMappings.value + headerMappingsValue,
    scalafmtOnCompile := true,
    trapExit := false,
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
    ),
    docker / dockerfile := {
      val artifact: File = assembly.value
      val artifactTargetPath = s"/app/${artifact.name}"
      new Dockerfile {
        from("pennsieve/java-cloudwrap:8-jre-alpine-0.5.9")
        copy(artifact, artifactTargetPath, chown="pennsieve:pennsieve")
        run("mkdir", "-p", "/home/pennsieve/.postgresql")
        run("wget", "-qO", "/home/pennsieve/.postgresql/root.crt", "https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem")
        // Reuse discover-service tag so this container has access to service's environment
        cmd("--service", "discover-service", "exec", "java", "-jar", artifactTargetPath)
      }
    },
    docker / imageNames := Seq(
      ImageName("pennsieve/discover-service-sync-elasticsearch:latest")
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @_*) => MergeStrategy.discard
      case PathList(ps @ _*) if ps.last endsWith "AWSCredentials.class"  => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith "AWSCredentialsProvider.class"  => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith "AWSSessionCredentials.class"  => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith "AWSSessionCredentialsProvider.class"  => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith "public-suffix-list.txt"  => MergeStrategy.first
      case PathList("codegen-resources", "service-2.json", _ @_*) => MergeStrategy.discard
      case PathList("codegen-resources", "paginators-1.json", _ @_*) => MergeStrategy.discard
      case PathList("codegen-resources", "customization.config", _ @_*) => MergeStrategy.discard
      case PathList("codegen-resources", "examples-1.json", _ @_*) => MergeStrategy.discard
      case PathList("codegen-resources", "waiters-2.json", _ @_*) => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },

  )
  .dependsOn(server)

lazy val client = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "discover-service-client",
    headerLicense := headerLicenseValue,
    headerMappings := headerMappings.value + headerMappingsValue,
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      "com.pennsieve" %% "core-models" % coreVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-java8" % circeVersion,
      "io.circe" %% "circe-jawn" % circeVersion,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
    ),
    publishTo := {
      val nexus = "https://nexus.pennsieve.cc/repository"
      if (isSnapshot.value) {
        Some("Nexus Realm" at s"$nexus/maven-snapshots")
      } else {
        Some("Nexus Realm" at s"$nexus/maven-releases")
      }
    },
    publishMavenStyle := true,
    Compile / guardrailTasks := List(
      ScalaClient(file("openapi/discover-service.yml"), pkg="com.pennsieve.discover.client"),
      ScalaClient(file("openapi/discover-service-internal.yml"), pkg="com.pennsieve.discover.client")
    ),
  )

lazy val scripts = project
  .enablePlugins(AutomateHeaderPlugin)
  .dependsOn(server)
  .settings(
    name := "discover-service-scripts",
    headerLicense := headerLicenseValue,
    headerMappings := headerMappings.value + headerMappingsValue,
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-jawn" % circeVersion,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.pennsieve" %% "service-utilities" % serviceUtilitiesVersion,
    )
  )

lazy val root = (project in file("."))
  .aggregate(server, client, syncElasticSearch, scripts)
