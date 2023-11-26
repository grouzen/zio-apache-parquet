import sbt._

object Dep {

  object V {
    val zio                   = "2.0.19"
    val zioSchema             = "0.4.16"
    val scalaCollectionCompat = "2.11.0"
  }

  object O {
    val scalaLang        = "org.scala-lang"
    val zio              = "dev.zio"
    val scalaLangModules = "org.scala-lang.modules"
  }

  lazy val zio                 = O.zio %% "zio"                   % V.zio
  lazy val zioSchema           = O.zio %% "zio-schema"            % V.zioSchema
  lazy val zioSchemaDerivation = O.zio %% "zio-schema-derivation" % V.zioSchema
  lazy val zioTest             = O.zio %% "zio-test"              % V.zio
  lazy val zioTestSbt          = O.zio %% "zio-test-sbt"          % V.zio

  lazy val scalaCollectionCompat = O.scalaLangModules %% "scala-collection-compat" % V.scalaCollectionCompat

  lazy val core = Seq(
    zio,
    zioSchema,
    zioSchemaDerivation,
    scalaCollectionCompat,
    zioTest    % Test,
    zioTestSbt % Test
  )

}
