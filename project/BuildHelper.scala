import sbt.*
import sbt.Keys.*
import scalafix.sbt.ScalafixPlugin.autoImport.scalafixSemanticdb

object BuildHelper {

  def stdSettings(projectName: String): Seq[Def.Setting[_]] = Seq(
    name              := s"zio-apache-parquet-$projectName",
    organization      := "me.mnedokushev",
    libraryDependencies ++= betterMonadicFor(scalaVersion.value),
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    Test / fork       := true
  )

  val Scala212 = "2.12.19"
  val Scala213 = "2.13.14"
  val Scala3   = "3.4.2"

  private def betterMonadicFor(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, _)) => Seq(compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"))
      case _            => Seq()
    }

}
