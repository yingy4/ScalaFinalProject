name := "PlayWS"
 
version := "1.0" 
      
lazy val `playws` = (project in file(".")).enablePlugins(PlayScala)

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"
      
resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
      
scalaVersion := "2.11.8"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies ++= Seq( jdbc , ehcache , ws , specs2 % Test , guice )
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"


unmanagedResourceDirectories in Test +=  (baseDirectory ( _ /"target/web/public/test" )).value

      