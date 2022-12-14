import CrossCompilationUtil.{
  getScalacOptions,
  getVersion,
  handle212OnlyDependency,
  scalaVersionMatch
}
ThisBuild / organization := "com.pennsieve"

lazy val scala212 = "2.12.11"
lazy val scala213 = "2.13.8"
lazy val supportedScalaVersions = List(scala212, scala213)

ThisBuild / scalaVersion := scala213

// Run tests in a separate JVM to prevent resource leaks.
ThisBuild / Test / fork := true

ThisBuild / resolvers ++= Seq(
  "Pennsieve Releases" at "https://nexus.pennsieve.cc/repository/maven-releases",
  "Pennsieve Snapshots" at "https://nexus.pennsieve.cc/repository/maven-snapshots"
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

lazy val headerLicenseValue = Some(
  HeaderLicense
    .Custom("Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.")
)
lazy val headerMappingsValue = HeaderFileType.scala -> HeaderCommentStyle.cppStyleLineComment

lazy val akkaHttpVersion = SettingKey[String]("akkaHttpVersion")
lazy val akkaVersion = SettingKey[String]("akkaVersion")
lazy val alpakkaVersion = SettingKey[String]("alpakkaVersion")

lazy val akkaHttp213Version = "10.2.9"
lazy val akka213Version = "2.6.19"
lazy val alpakka213Version = "4.0.0"
lazy val akkaHttp212Version = "10.1.11"
lazy val akka212Version = "2.6.5"
lazy val alpakka212Version = "1.1.0"

lazy val circeVersion = SettingKey[String]("circeVersion")
lazy val circe212Version = "0.11.1"
lazy val circe213Version = "0.14.1"

lazy val coreVersion = "166-27f7fae"
lazy val authMiddlewareVersion = "5.1.3"
lazy val serviceUtilitiesVersion = "8-9751ee3"
lazy val utilitiesVersion = "4-55953e4"
lazy val doiServiceClientVersion = "12-756107b"
lazy val slickVersion = "3.3.3"
lazy val slickPgVersion = "0.20.3"
lazy val dockerItVersion = "0.9.9"
lazy val logbackVersion = "1.2.3"
lazy val awsSdkVersion = "2.10.56"
lazy val pureConfigVersion = "0.17.1"
lazy val elastic4sVersion = "7.12.3"
lazy val catsVersion = "2.0.0"
lazy val jacksonVersion = "2.9.6"

lazy val Integration = config("integration") extend (Test)

lazy val server = project
  .dependsOn(client % "test")
  .enablePlugins(AutomateHeaderPlugin)
  .enablePlugins(DockerPlugin)
  .configs(Integration)
  .settings(
    name := "discover-service",
    headerLicense := headerLicenseValue,
    headerMappings := headerMappings.value + headerMappingsValue,
    scalacOptions ++= getScalacOptions(scalaVersion.value),
    scalafmtOnCompile := true,
    assembly / test := {},
    Test / fork := true,
    Test / testForkedParallel := true,
    // Only run integration tests with the `integration:test` command
    inConfig(Integration)(Defaults.testTasks),
    Test / testOptions := Seq(
      Tests.Filter(!_.toLowerCase.contains("integration"))
    ),
    Integration / testOptions := Seq(
      Tests.Filter(_.toLowerCase.contains("integration"))
    ),
    Compile / mainClass := Some("com.pennsieve.discover.Server"),
    circeVersion := getVersion(
      scalaVersion.value,
      circe212Version,
      circe213Version
    ),
    akkaVersion := getVersion(
      scalaVersion.value,
      akka212Version,
      akka213Version
    ),
    akkaHttpVersion := getVersion(
      scalaVersion.value,
      akkaHttp212Version,
      akkaHttp213Version
    ),
    alpakkaVersion := getVersion(
      scalaVersion.value,
      alpakka212Version,
      alpakka213Version
    ),
    /*dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
    ),*/
    libraryDependencies ++= Seq(
      "com.pennsieve" %% "service-utilities" % serviceUtilitiesVersion,
      "com.pennsieve" %% "utilities" % utilitiesVersion,
      "com.pennsieve" %% "auth-middleware" % authMiddlewareVersion,
      "com.pennsieve" %% "core-models" % coreVersion,
      "com.pennsieve" %% "doi-service-client" % doiServiceClientVersion,
      "org.typelevel" %% "cats-core" % catsVersion,
      "io.circe" %% "circe-core" % circeVersion.value,
      "io.circe" %% "circe-generic" % circeVersion.value,
      "commons-io" % "commons-io" % "2.6",
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion.value,
      "com.typesafe.akka" %% "akka-http-xml" % akkaHttpVersion.value,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion.value,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion.value,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion.value,
      "com.lightbend.akka" %% "akka-stream-alpakka-sqs" % alpakkaVersion.value,
      "com.lightbend.akka" %% "akka-stream-alpakka-s3" % alpakkaVersion.value,
      "com.lightbend.akka" %% "akka-stream-alpakka-awslambda" % alpakkaVersion.value,
      "com.lightbend.akka" %% "akka-stream-alpakka-csv" % alpakkaVersion.value,
      "software.amazon.awssdk" % "lambda" % awsSdkVersion,
      "software.amazon.awssdk" % "sfn" % awsSdkVersion,
      "software.amazon.awssdk" % "sns" % awsSdkVersion,
      "software.amazon.awssdk" % "s3" % awsSdkVersion,
      "software.amazon.awssdk" % "sts" % awsSdkVersion,
      "software.amazon.awssdk" % "url-connection-client" % awsSdkVersion,
      "com.github.pureconfig" %% "pureconfig" % pureConfigVersion,
      "com.github.pureconfig" %% "pureconfig-squants" % pureConfigVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "com.typesafe.slick" %% "slick" % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp" % slickVersion,
      "com.github.tminglei" %% "slick-pg" % slickPgVersion,
      "com.github.tminglei" %% "slick-pg_circe-json" % slickPgVersion,
      "org.postgresql" % "postgresql" % "42.2.4",
      "com.zaxxer" % "HikariCP" % "3.3.1",
      "io.scalaland" %% "chimney" % "0.6.1",
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "ch.qos.logback" % "logback-core" % logbackVersion,
      "net.logstash.logback" % "logstash-logback-encoder" % "5.2",
      "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
      "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % elastic4sVersion,
      "com.sksamuel.elastic4s" %% "elastic4s-json-circe" % elastic4sVersion,
      "org.apache.commons" % "commons-lang3" % "3.9",
      "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.0",
      "org.scalikejdbc" %% "scalikejdbc" % "3.4.0",
      "com.zaneli" %% "scalikejdbc-athena" % "0.2.4",
      // Test dependencies
      "org.scalatest" %% "scalatest" % "3.2.12" % Test,
      "com.whisk" %% "docker-testkit-scalatest" % dockerItVersion % Test,
      "com.whisk" %% "docker-testkit-impl-spotify" % dockerItVersion % Test,
      "com.pennsieve" %% "utilities" % utilitiesVersion % "test" classifier "tests",
      "com.pennsieve" %% "service-utilities" % serviceUtilitiesVersion % "test" classifier "tests",
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion.value % Test,
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion.value % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion.value % Test,
      "org.apache.httpcomponents" % "httpclient" % "4.5.8" % Test,
      "software.amazon.awssdk" % "s3" % awsSdkVersion % Test,
      "com.sksamuel.elastic4s" %% "elastic4s-testkit" % elastic4sVersion % Test,
      "org.mock-server" % "mockserver-client-java-no-dependencies" % "5.14.0" % Test
    ),
    Compile / guardrailTasks := List(
      ScalaServer(
        file("openapi/discover-service.yml"),
        pkg = "com.pennsieve.discover.server"
      ),
      ScalaServer(
        file("openapi/discover-service-internal.yml"),
        pkg = "com.pennsieve.discover.server"
      )
    ),
    docker / dockerfile := {
      val artifact: File = assembly.value
      val artifactTargetPath = s"/app/${artifact.name}"
      new Dockerfile {
        from("pennsieve/java-cloudwrap:10-jre-slim-0.5.9")
        copy(artifact, artifactTargetPath, chown = "pennsieve:pennsieve")
        copy(
          baseDirectory.value / "bin" / "run.sh",
          "/app/run.sh",
          chown = "pennsieve:pennsieve"
        )
        run("mkdir", "-p", "/home/pennsieve/.postgresql")
        run(
          "wget",
          "-qO",
          "/home/pennsieve/.postgresql/root.crt",
          "https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem"
        )
        run(
          "wget",
          "-qO",
          "/app/newrelic.jar",
          "http://download.newrelic.com/newrelic/java-agent/newrelic-agent/current/newrelic.jar"
        )
        run(
          "wget",
          "-qO",
          "/app/newrelic.yml",
          "http://download.newrelic.com/newrelic/java-agent/newrelic-agent/current/newrelic.yml"
        )
        cmd(
          "--service",
          "discover-service",
          "exec",
          "app/run.sh",
          artifactTargetPath
        )
      }
    },
    docker / imageNames := Seq(ImageName("pennsieve/discover-service:latest")),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @_*) => MergeStrategy.discard
      case PathList(ps @ _*) if ps.last endsWith "AWSCredentials.class" =>
        MergeStrategy.first
      case PathList(ps @ _*)
          if ps.last endsWith "AWSCredentialsProvider.class" =>
        MergeStrategy.first
      case PathList(ps @ _*)
          if ps.last endsWith "AWSSessionCredentials.class" =>
        MergeStrategy.first
      case PathList(ps @ _*)
          if ps.last endsWith "AWSSessionCredentialsProvider.class" =>
        MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith "public-suffix-list.txt" =>
        MergeStrategy.first
      case PathList("codegen-resources", "service-2.json", _ @_*) =>
        MergeStrategy.discard
      case PathList("codegen-resources", "paginators-1.json", _ @_*) =>
        MergeStrategy.discard
      case PathList("codegen-resources", "customization.config", _ @_*) =>
        MergeStrategy.discard
      case PathList("codegen-resources", "examples-1.json", _ @_*) =>
        MergeStrategy.discard
      case PathList("codegen-resources", "waiters-2.json", _ @_*) =>
        MergeStrategy.discard
      case PathList("module-info.class") => MergeStrategy.discard
      case x if x.endsWith("/module-info.class") => MergeStrategy.discard
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
    coverageMinimumStmtTotal := 70,
    coverageFailOnMinimum := true
  )

lazy val syncElasticSearch = project
  .enablePlugins(AutomateHeaderPlugin)
  .enablePlugins(DockerPlugin)
  .settings(
    name := "discover-service-sync-elasticsearch",
    headerLicense := headerLicenseValue,
    headerMappings := headerMappings.value + headerMappingsValue,
    scalacOptions ++= getScalacOptions(scalaVersion.value),
    scalafmtOnCompile := true,
    trapExit := false,
    /*dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
    ),*/
    docker / dockerfile := {
      val artifact: File = assembly.value
      val artifactTargetPath = s"/app/${artifact.name}"
      new Dockerfile {
        from("pennsieve/java-cloudwrap:8-jre-alpine-0.5.9")
        copy(artifact, artifactTargetPath, chown = "pennsieve:pennsieve")
        run("mkdir", "-p", "/home/pennsieve/.postgresql")
        run(
          "wget",
          "-qO",
          "/home/pennsieve/.postgresql/root.crt",
          "https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem"
        )
        // Reuse discover-service tag so this container has access to service's environment
        cmd(
          "--service",
          "discover-service",
          "exec",
          "java",
          "-jar",
          artifactTargetPath
        )
      }
    },
    docker / imageNames := Seq(
      ImageName("pennsieve/discover-service-sync-elasticsearch:latest")
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @_*) => MergeStrategy.discard
      case PathList(ps @ _*) if ps.last endsWith "AWSCredentials.class" =>
        MergeStrategy.first
      case PathList(ps @ _*)
          if ps.last endsWith "AWSCredentialsProvider.class" =>
        MergeStrategy.first
      case PathList(ps @ _*)
          if ps.last endsWith "AWSSessionCredentials.class" =>
        MergeStrategy.first
      case PathList(ps @ _*)
          if ps.last endsWith "AWSSessionCredentialsProvider.class" =>
        MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith "public-suffix-list.txt" =>
        MergeStrategy.first
      case PathList("codegen-resources", "service-2.json", _ @_*) =>
        MergeStrategy.discard
      case PathList("codegen-resources", "paginators-1.json", _ @_*) =>
        MergeStrategy.discard
      case PathList("codegen-resources", "customization.config", _ @_*) =>
        MergeStrategy.discard
      case PathList("codegen-resources", "examples-1.json", _ @_*) =>
        MergeStrategy.discard
      case PathList("codegen-resources", "waiters-2.json", _ @_*) =>
        MergeStrategy.discard
      case PathList("module-info.class") => MergeStrategy.discard
      case x if x.endsWith("/module-info.class") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )
  .dependsOn(server)

lazy val client = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "discover-service-client",
    headerLicense := headerLicenseValue,
    headerMappings := headerMappings.value + headerMappingsValue,
    scalacOptions ++= getScalacOptions(scalaVersion.value),
    crossScalaVersions := supportedScalaVersions,
    circeVersion := getVersion(
      scalaVersion.value,
      circe212Version,
      circe213Version
    ),
    akkaVersion := getVersion(
      scalaVersion.value,
      akka212Version,
      akka213Version
    ),
    akkaHttpVersion := getVersion(
      scalaVersion.value,
      akkaHttp212Version,
      akkaHttp213Version
    ),
    libraryDependencies ++= Seq(
      "com.pennsieve" %% "core-models" % coreVersion,
      "io.circe" %% "circe-core" % circeVersion.value,
      "io.circe" %% "circe-generic" % circeVersion.value,
      "io.circe" %% "circe-jawn" % circeVersion.value,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion.value,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion.value,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion.value,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion.value
    ),
    libraryDependencies ++= handle212OnlyDependency(
      scalaVersion.value,
      "io.circe" %% "circe-java8" % circeVersion.value
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
    Compile / guardrailTasks := scalaVersionMatch(
      scalaVersion.value,
      List(
        ScalaClient(
          file("openapi/discover-service.yml"),
          pkg = "com.pennsieve.discover.client",
          modules = List("akka-http", "circe-0.11")
        ),
        ScalaClient(
          file("openapi/discover-service-internal.yml"),
          pkg = "com.pennsieve.discover.client",
          modules = List("akka-http", "circe-0.11")
        )
      ),
      List(
        ScalaClient(
          file("openapi/discover-service.yml"),
          pkg = "com.pennsieve.discover.client"
        ),
        ScalaClient(
          file("openapi/discover-service-internal.yml"),
          pkg = "com.pennsieve.discover.client"
        )
      )
    )
  )

lazy val scripts = project
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(ScoverageSbtPlugin)
  .dependsOn(server)
  .settings(
    name := "discover-service-scripts",
    headerLicense := headerLicenseValue,
    headerMappings := headerMappings.value + headerMappingsValue,
    scalacOptions ++= getScalacOptions(scalaVersion.value),
    circeVersion := getVersion(
      scalaVersion.value,
      circe212Version,
      circe213Version
    ),
    akkaVersion := getVersion(
      scalaVersion.value,
      akka212Version,
      akka213Version
    ),
    akkaHttpVersion := getVersion(
      scalaVersion.value,
      akkaHttp212Version,
      akkaHttp213Version
    ),
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % circeVersion.value,
      "io.circe" %% "circe-generic" % circeVersion.value,
      "io.circe" %% "circe-jawn" % circeVersion.value,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion.value,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion.value,
      "com.pennsieve" %% "service-utilities" % serviceUtilitiesVersion
    )
  )

lazy val root = (project in file("."))
  .aggregate(server, client, syncElasticSearch, scripts)
  .settings(
    // crossScalaVersions must be set to Nil on the aggregating project
    crossScalaVersions := Nil,
    publish / skip := true
  )
