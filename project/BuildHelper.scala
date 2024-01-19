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
    Test / fork       := true,
    Test / unmanagedSourceDirectories ++= crossVersionSources(scalaVersion.value, "test", baseDirectory.value)
  )

  val Scala212 = "2.12.19"
  val Scala213 = "2.13.13"
  val Scala3   = "3.3.3"

  private def betterMonadicFor(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, _)) => Seq(compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"))
      case _            => Seq()
    }

  def crossVersionSources(scalaVersion: String, conf: String, baseDirectory: File): List[File] = {
    val versions = CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 12)) =>
        List("2.12")
      case Some((2, 13)) =>
        List("2.13+")
      case Some((3, _))  =>
        List("2.13+")
      case _             =>
        List.empty
    }

    for {
      version <- "scala" :: versions.map("scala-" + _)
      file     = baseDirectory / "src" / conf / version if file.exists
    } yield file
  }

}
