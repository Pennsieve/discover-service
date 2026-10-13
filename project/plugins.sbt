// Until https://github.com/coursier/coursier/issues/1815 is fixed
useCoursier := false

addSbtPlugin("dev.guardrail" % "sbt-guardrail" % "0.70.0.2")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.11.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.3.1")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.0.0")

addDependencyTreePlugin

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

addSbtPlugin("no.arktekk.sbt" % "aether-deploy" % "0.31.0")
