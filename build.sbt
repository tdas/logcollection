name := "LogCollection"

scalaVersion := "2.10.3"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "org.apache.commons" % "commons-io" % "1.3.2"

libraryDependencies ++= Seq(
    "org.slf4j"                 % "slf4j-api"          % "1.7.2",
    "org.apache.logging.log4j"  % "log4j-slf4j-impl"   % "2.0-rc1",
    "org.apache.logging.log4j"  % "log4j-core"         % "2.0-rc1"
)

fork in Test := true
