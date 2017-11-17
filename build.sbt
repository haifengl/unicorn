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
  .aggregate(util, oid, json, bigtable, hbase, cassandra, accumulo, rocksdb, unibase, narwhal, sql, shell)

lazy val util = project.in(file("util")).settings(commonSettings: _*)

lazy val oid = project.in(file("oid")).settings(commonSettings: _*).dependsOn(util)

lazy val json = project.in(file("json")).settings(commonSettings: _*).dependsOn(oid)

lazy val bigtable = project.in(file("bigtable")).settings(commonSettings: _*).dependsOn(util)

lazy val hbase = project.in(file("hbase")).settings(commonSettings: _*).dependsOn(bigtable)

lazy val accumulo = project.in(file("accumulo")).settings(commonSettings: _*).dependsOn(bigtable)

lazy val cassandra = project.in(file("cassandra")).settings(commonSettings: _*).dependsOn(bigtable, util)

lazy val rocksdb = project.in(file("rocksdb")).settings(commonSettings: _*).dependsOn(bigtable, util)

//lazy val index = project.in(file("index")).settings(nonPubishSettings: _*).dependsOn(bigtable, json, hbase % "test")

lazy val unibase = project.in(file("unibase")).settings(commonSettings: _*).dependsOn(json, oid, bigtable, accumulo % "test")

lazy val narwhal = project.in(file("narwhal")).settings(commonSettings: _*).dependsOn(unibase, hbase)

lazy val sql = project.in(file("sql")).settings(commonSettings: _*).dependsOn(util, narwhal)

//lazy val transaction = project.in(file("transaction")).settings(commonSettings: _*).dependsOn(util, narwhal)

//lazy val search = project.in(file("search")).settings(nonPubishSettings: _*).dependsOn(unibase)

lazy val shell = project.in(file("shell")).settings(nonPubishSettings: _*).dependsOn(unibase, narwhal, sql, rocksdb)

lazy val rhino = project.in(file("rhino")).enablePlugins(SbtTwirl).settings(nonPubishSettings: _*).dependsOn(unibase, narwhal)

