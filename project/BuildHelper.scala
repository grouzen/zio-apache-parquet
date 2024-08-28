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
    Test / unmanagedSourceDirectories ++= crossVersionSources(scalaVersion.value, "test", baseDirectory.value),
    Test / unmanagedSourceDirectories ++= crossVersionSources(scalaVersion.value, "main", baseDirectory.value),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, _)) =>
          Seq(Dep.scalaReflect.value)
        case _            => Seq.empty
      }
    }
  )

  val Scala212 = "2.12.19"
  val Scala213 = "2.13.14"
  val Scala3   = "3.5.0"

  private def betterMonadicFor(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, _)) => Seq(compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"))
      case _            => Seq()
    }

  def crossVersionSources(scalaVersion: String, conf: String, baseDirectory: File): List[File] = {
    val versions = CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 12)) =>
        List("2", "2.12")
      case Some((2, 13)) =>
        List("2", "2.13", "2.13+")
      case Some((3, _))  =>
        List("2.13+", "3")
      case _             =>
        List.empty
    }

    for {
      version <- "scala" :: versions.map("scala-" + _)
      file     = baseDirectory / "src" / conf / version if file.exists
    } yield file
  }

}
