import sbt._
import sbt.Keys.scalaVersion

object Dep {

  object V {
    val zio                   = "2.1.23"
    val zioSchema             = "1.7.5"
    val scalaCollectionCompat = "2.13.0"
    val apacheParquet         = "1.16.0"
    val apacheHadoop          = "3.4.2"
  }

  object O {
    val apacheParquet    = "org.apache.parquet"
    val apacheHadoop     = "org.apache.hadoop"
    val zio              = "dev.zio"
    val scalaLang        = "org.scala-lang"
    val scalaLangModules = s"$scalaLang.modules"
  }

  lazy val zio                 = O.zio %% "zio"                   % V.zio
  lazy val zioSchema           = O.zio %% "zio-schema"            % V.zioSchema
  lazy val zioSchemaDerivation = O.zio %% "zio-schema-derivation" % V.zioSchema
  lazy val zioTest             = O.zio %% "zio-test"              % V.zio
  lazy val zioTestSbt          = O.zio %% "zio-test-sbt"          % V.zio

  lazy val parquetHadoop = O.apacheParquet % "parquet-hadoop" % V.apacheParquet
  lazy val parquetColumn = O.apacheParquet % "parquet-column" % V.apacheParquet

  lazy val hadoopCommon = O.apacheHadoop % "hadoop-common" % V.apacheHadoop
  lazy val hadoopMapred = O.apacheHadoop % "hadoop-mapred" % "0.22.0"

  lazy val scalaCollectionCompat = O.scalaLangModules %% "scala-collection-compat" % V.scalaCollectionCompat

  lazy val scalaReflect = Def.setting("org.scala-lang" % "scala-reflect" % scalaVersion.value % "provided")

  lazy val core = Seq(
    zio,
    zioSchema,
    zioSchemaDerivation,
    scalaCollectionCompat,
    parquetHadoop,
    parquetColumn,
    zioTest    % Test,
    zioTestSbt % Test
  )

  lazy val hadoop = Seq(
    hadoopCommon,
    hadoopMapred,
    zioTest    % Test,
    zioTestSbt % Test
  )

}
