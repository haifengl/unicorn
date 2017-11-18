name := "unicorn-shell"

mainClass in Compile := Some("unicorn.shell.Main")

// native packager
enablePlugins(JavaAppPackaging)

maintainer := "Haifeng Li <haifeng.hli@gmail.com>"

packageName := "unicorn"

packageSummary := "Unicorn"

packageDescription := "Unicorn"

executableScriptName := "unicorn"

bashScriptConfigLocation := Some("${app_home}/../conf/unicorn.ini")

bashScriptExtraDefines += """addJava "-Dscala.repl.autoruncode=${app_home}/init.scala""""

bashScriptExtraDefines += """addJava "-Dconfig.file=${app_home}/../conf/unicorn.conf""""

batScriptExtraDefines  += """set _JAVA_OPTS=!_JAVA_OPTS! -Dscala.repl.autoruncode=%UNICORN_HOME%\\bin\\init.scala -Dconfig.file=%UNICORN_HOME%\\..\\conf\\unicorn.conf"""

// native packager Docker plugin
enablePlugins(DockerPlugin)

dockerBaseImage := "dajobe/hbase"

packageName in Docker := "haifengl/unicorn"

dockerUpdateLatest := true

// BuildInfo
enablePlugins(BuildInfoPlugin)

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)

buildInfoPackage := "unicorn.shell"

buildInfoOptions += BuildInfoOption.BuildTime

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.11.11"

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.18"
