scalaVersion := "2.13.5"

organization := "com.pirum"
organizationName := "Pirum Systems"
organizationHomepage := Some(url("https://www.pirum.com"))

lazy val scalatestVersion = "3.2.9"
lazy val akkaVersion = "2.6.17"

libraryDependencies ++= {
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.14.1",
    "org.apache.logging.log4j" % "log4j-core" % "2.14.1",
    "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
    "org.scalatest" %% "scalatest" % scalatestVersion % Test,
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.14.1" % Test
  )
}

// Stop tests that rely on threads being available running over one another
parallelExecution in Test := false
