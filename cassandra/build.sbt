name := "unicorn-cassandra"

libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "2.2.11" exclude("ch.qos.logback", "logback-classic")

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"