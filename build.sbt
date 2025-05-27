lazy val `scala-quartz` = project
  .in(file("."))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= List(
      "org.quartz-scheduler" % "quartz" % "2.5.0",
      "org.tpolecat" %% "doobie-core" % "1.0.0-RC5",
      "io.circe" %% "circe-jawn" % "0.14.13",
    ),
  )

lazy val commonSettings: List[Def.Setting[_]] = DecentScala.decentScalaSettings ++ List(
  crossScalaVersions -= DecentScala.decentScalaVersion212,
  organization := "com.github.sideeffffect",
  homepage := Some(url("https://github.com/sideeffffect/scala-quartz")),
  licenses := List("APLv2" -> url("https://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      "sideeffffect",
      "Ondra Pelech",
      "ondra.pelech@gmail.com",
      url("https://github.com/sideeffffect"),
    ),
  ),
  missinglinkExcludedDependencies ++= List(
//    moduleFilter(organization = "com.zaxxer", name = "HikariCP"),
//    moduleFilter(organization = "dev.zio", name = "zio-interop-cats_2.12"), // depends on zio-managed
//    moduleFilter(organization = "dev.zio", name = "zio-interop-cats_2.13"),
//    moduleFilter(organization = "org.slf4j", name = "slf4j-api"),
  ),
  missinglinkIgnoreDestinationPackages ++= List(
    IgnoredPackage("jakarta.transaction"),
//    IgnoredPackage("org.osgi.framework"),
//    IgnoredPackage("java.sql"), // https://github.com/tpolecat/doobie/pull/1632
  ),
  mimaBinaryIssueFilters ++= List(
  ),
)

addCommandAlias("ci", "; check; +publishLocal")
