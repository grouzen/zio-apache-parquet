import BuildHelper._
import sbt.librarymanagement.DependencyFilter
import org.typelevel.scalacoptions.ScalacOptions

inThisBuild(
  List(
    organization                        := "me.mnedokushev",
    homepage                            := Some(uri("https://github.com/grouzen/zio-apache-parquet")),
    licenses                            := List("Apache-2.0" -> uri("http://www.apache.org/licenses/LICENSE-2.0")),
    developers                          := List(
      Developer(
        "grouzen",
        "Mykhailo Nedokushev",
        "michael.nedokushev@gmail.com",
        uri("https://github.com/grouzen")
      )
    ),
    scmInfo                             := Some(
      ScmInfo(
        uri("https://github.com/grouzen/zio-apache-parquet"),
        "scm:git:git@github.com:grouzen/zio-apache-parquet.git"
      )
    ),
    crossScalaVersions                  := Seq(Scala213, Scala3),
    scalaVersion                        := Scala3,
    githubWorkflowJavaVersions          := Seq(JavaSpec.temurin("17")),
    githubWorkflowPublishTargetBranches := Seq(),
    githubWorkflowBuildPreamble         := Seq(
      WorkflowStep.Sbt(
        List(
          "scalafix --check",
          "scalafmtCheckAll"
        ),
        name = Some("Lint Scala code")
      ),
      WorkflowStep.Sbt(
        List(
          "all undeclaredCompileDependenciesTest",
          "all unusedCompileDependenciesTest"
        ),
        name = Some("Check explicit dependencies")
      )
    )
  )
)

lazy val root =
  project
    .in(file("."))
    .aggregate(core, hadoop)
    .settings(publish / skip := true)
    .settings(
      addCommandAlias("fmtAll", "+scalafmtAll; +scalafixAll")
    )

lazy val core =
  project
    .in(file("modules/core"))
    .settings(
      stdSettings("core"),
      undeclaredCompileDependenciesFilter -= DependencyFilter.moduleFilter("dev.zio", "zio-stacktracer"),
      tpolecatSettings,
      libraryDependencies ++= Dep.core,
      libraryDependencies ++= {
        CrossVersion.partialVersion(scalaVersion.value) match {
          case Some((2, _)) => Seq(Dep.scalaReflect.value)
          case _            => Seq.empty
        }
      },
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
    )

lazy val hadoop =
  project
    .in(file("modules/hadoop"))
    .settings(
      stdSettings("hadoop"),
      Test / exportJars        := false,
      Test / fork              := false,
      Test / closeClassLoaders := false,
      undeclaredCompileDependenciesFilter -= DependencyFilter.moduleFilter("dev.zio", "izumi-reflect"),
      undeclaredCompileDependenciesFilter -= DependencyFilter.moduleFilter("dev.zio", "zio-stacktracer"),
      tpolecatSettings,
      libraryDependencies ++= Dep.hadoop,
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
    )
    .dependsOn(core % "compile->compile;test->test")

val tpolecatSettings =
  Seq(
    tpolecatScalacOptions += ScalacOptions.source3,
    tpolecatExcludeOptions += ScalacOptions.lintInferAny
  )

val benchmarkSettings = Seq(
  publish / skip       := true,
  crossScalaVersions   := Seq(Scala3),
  Compile / run / fork := true
)

lazy val benchmarks =
  project
    .in(file("modules/benchmarks"))
    .enablePlugins(JmhPlugin)
    .dependsOn(hadoop)
    .settings(benchmarkSettings)
    .settings(
      libraryDependencies ++= Dep.benchmarks
    )

lazy val benchmarksFs2 =
  project
    .in(file("modules/benchmarks-fs2"))
    .enablePlugins(JmhPlugin)
    .dependsOn(benchmarks)
    .settings(benchmarkSettings)
    .settings(
      libraryDependencies ++= Dep.benchmarksFs2
    )

lazy val benchmarksAkka =
  project
    .in(file("modules/benchmarks-akka"))
    .enablePlugins(JmhPlugin)
    .dependsOn(benchmarks)
    .settings(benchmarkSettings)
    .settings(
      libraryDependencies += Dep.parquet4sAkka
    )

lazy val benchmarksPekko =
  project
    .in(file("modules/benchmarks-pekko"))
    .enablePlugins(JmhPlugin)
    .dependsOn(benchmarks)
    .settings(benchmarkSettings)
    .settings(
      libraryDependencies += Dep.parquet4sPekko
    )
