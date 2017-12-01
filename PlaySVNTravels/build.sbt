name := "PlayWS"
 
version := "1.0" 
      
lazy val `playws` = (project in file(".")).enablePlugins(PlayScala)

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"
      
resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
      
scalaVersion := "2.12.2"

libraryDependencies ++= Seq( jdbc , ehcache , ws , specs2 % Test , guice )
libraryDependencies ++= Seq("com.typesafe.akka" %% "akka-http" % "10.0.10"
                            ,"com.typesafe.akka" %% "akka-stream" % "2.5.7"
                            ,"org.apache.kafka" % "kafka_2.10" % "0.8.2.1"
)

unmanagedResourceDirectories in Test +=  (baseDirectory ( _ /"target/web/public/test" )).value

      