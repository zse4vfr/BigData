ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.13.12"

ThisBuild / libraryDependencies += "org.apache.zookeeper"%"zookeeper"% "3.8.3"%"compile"

lazy val root = (project in file("."))
  .settings(
    name := "lr-zookeeper"
  )
