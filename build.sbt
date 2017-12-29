name := "unicorn"

import com.typesafe.sbt.pgp.PgpKeys.{useGpg, publishSigned, publishLocalSigned}

lazy val commonSettings = Seq(
  organization := "com.github.haifengl",
  organizationName := "Haifeng Li",
  organizationHomepage := Some(url("http://haifengl.github.io/")),
  version := "3.0.0",
  scalaVersion := "2.11.11",
  scalacOptions := Seq("-feature", "-language:_", "-unchecked", "-deprecation", "-encoding", "utf8"),
  scalacOptions in Test ++= Seq("-Yrangepos"),
  libraryDependencies += "org.specs2" %% "specs2-core" % "3.8.9" % "test",
  parallelExecution in Test := false,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  publishArtifact in Test := false ,
  publishMavenStyle := true,
  useGpg := true,
  pomIncludeRepository := { _ => false },
  pomExtra := (
    <url>https://github.com/haifengl/unicorn</url>
      <licenses>
        <license>
          <name>Apache License, Version 2.0</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:haifengl/unicorn.git</url>
        <connection>scm:git:git@github.com:haifengl/unicorn.git</connection>
      </scm>
      <developers>
        <developer>
          <id>haifengl</id>
          <name>Haifeng Li</name>
          <url>http://haifengl.github.io/</url>
        </developer>
      </developers>
    )
)

lazy val nonPubishSettings = commonSettings ++ Seq(
  publishArtifact := false,
  publishLocal := {},
  publish := {},
  publishSigned := {},
  publishLocalSigned := {}
)

lazy val root = project.in(file(".")).settings(nonPubishSettings: _*)
  .aggregate(snowflake, json, kv, bigtable, hbase, cassandra, accumulo, rocksdb, unibase, shell)

lazy val json = project.in(file("json")).settings(commonSettings: _*)

lazy val snowflake = project.in(file("snowflake")).settings(commonSettings: _*)

lazy val kv = project.in(file("kv")).settings(commonSettings: _*)

lazy val rocksdb = project.in(file("rocksdb")).settings(commonSettings: _*).dependsOn(kv)

lazy val bigtable = project.in(file("bigtable")).settings(commonSettings: _*)

lazy val hbase = project.in(file("hbase")).settings(commonSettings: _*).dependsOn(bigtable)

lazy val accumulo = project.in(file("accumulo")).settings(commonSettings: _*).dependsOn(bigtable)

lazy val cassandra = project.in(file("cassandra")).settings(commonSettings: _*).dependsOn(bigtable)

lazy val unibase = project.in(file("unibase")).settings(commonSettings: _*).dependsOn(json, bigtable, kv, snowflake % "test", accumulo % "test")

lazy val narwhal = project.in(file("narwhal")).settings(commonSettings: _*).dependsOn(unibase, hbase)

lazy val shell = project.in(file("shell")).settings(nonPubishSettings: _*).dependsOn(unibase)

//lazy val rhino = project.in(file("rhino")).enablePlugins(SbtTwirl).settings(nonPubishSettings: _*).dependsOn(narwhal)

