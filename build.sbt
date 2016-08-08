name := "scylla-spark-example-simple"
version := "1.0"
scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
        "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-M1",
        "org.apache.spark" %% "spark-catalyst" % "1.5.0" % "provided"
    )

javaOptions ++= Seq("-Xmx5G", "-XX:MaxPermSize=5G", "-XX:+CMSClassUnloadingEnabled")