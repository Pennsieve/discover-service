resolvers ++= Seq(
  "pennsieve-maven-proxy" at "https://nexus.pennsieve.cc/repository/maven-public",
  Resolver.url(
    "pennsieve-ivy-proxy",
    url("https://nexus.pennsieve.cc/repository/ivy-public/")
  )(
    Patterns(
      "[organization]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]"
    )
  ),
)

credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "nexus.pennsieve.cc",
  sys.env.getOrElse("PENNSIEVE_NEXUS_USER", "pennsieveci"),
  sys.env.getOrElse("PENNSIEVE_NEXUS_PW", "")
)

// Until https://github.com/coursier/coursier/issues/1815 is fixed
useCoursier := false

addSbtPlugin("dev.guardrail" % "sbt-guardrail" % "0.70.0.2")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.11.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.9.3")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.0.0")

addDependencyTreePlugin

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
