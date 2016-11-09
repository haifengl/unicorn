name := "unicorn-narwhal"

libraryDependencies ++= {
  val sparkV = "1.6.1"
  Seq(
    "org.apache.spark"  %%  "spark-core"    % sparkV  exclude("org.slf4j", "slf4j-log4j12"),
    "org.apache.spark"  %%  "spark-sql"     % sparkV,
    "org.apache.spark"  %%  "spark-graphx"  % sparkV,
    "org.slf4j"         %   "slf4j-simple"  % "1.7.21" % "test"
  )
}



