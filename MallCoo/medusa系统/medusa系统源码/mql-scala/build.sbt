assemblySettings

name := "mql-scala"

version := "1.0"

scalaVersion := "2.12.1"

libraryDependencies ++= {
  val akkaVersion = "2.5.1"
  val akkaHttpVersion = "10.0.7"
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
    "com.iheart" %% "ficus" % "1.4.0",
    //    "org.json4s" %% "json4s-native" % "3.5.1",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "ch.qos.logback" % "logback-core" % "1.2.3",
    "org.codehaus.janino" % "janino" % "3.0.6",
    "com.sun.jdmk" % "jmxtools" % "1.2.1",
    //    "joda-time" % "joda-time" % "2.9.4",
    //    "org.joda" % "joda-convert" % "1.8.1",
    "com.github.cb372" %% "scalacache-ehcache" % "0.9.3",
    "net.sf.ehcache" % "ehcache" % "2.10.4",
    "com.google.guava" % "guava" % "21.0",
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "org.scalikejdbc" %% "scalikejdbc-test"   % "3.0.0"   % "test",
    //        "com.alibaba" % "fastjson" % "1.2.28"
    "net.minidev" % "json-smart" % "2.2.1",
    "com.zaxxer" % "HikariCP" % "2.6.2",
    "org.scalikejdbc" %% "scalikejdbc" % "3.0.0",
    "org.scalikejdbc" %% "scalikejdbc-config"  % "3.0.0"
  )
}