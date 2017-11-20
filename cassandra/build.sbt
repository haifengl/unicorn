name := "unicorn-cassandra"

libraryDependencies += "org.apache.cassandra" % "cassandra-thrift" % "3.11.1" exclude("ch.qos.logback", "logback-classic")

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"