import sbt._
import sbt.Keys.scalaVersion

object Dep {

  object V {
    val zio           = "2.1.26"
    val zioSchema     = "1.8.5"
    val zioPrelude    = "1.0.0-RC47"
    val zioStreams    = "2.1.24"
    val apacheParquet = "1.18.1"
    val apacheHadoop  = "3.5.0"
  }

  object O {
    val apacheParquet = "org.apache.parquet"
    val apacheHadoop  = "org.apache.hadoop"
    val zio           = "dev.zio"
    val scalaLang     = "org.scala-lang"
  }

  lazy val zio                 = O.zio %% "zio"                   % V.zio
  lazy val zioSchema           = O.zio %% "zio-schema"            % V.zioSchema
  lazy val zioSchemaDerivation = O.zio %% "zio-schema-derivation" % V.zioSchema
  lazy val zioPrelude          = O.zio %% "zio-prelude"           % V.zioPrelude
  lazy val zioStreams          = O.zio %% "zio-streams"           % V.zioStreams
  lazy val zioTest             = O.zio %% "zio-test"              % V.zio
  lazy val zioTestSbt          = O.zio %% "zio-test-sbt"          % V.zio

  lazy val parquetHadoop = O.apacheParquet % "parquet-hadoop" % V.apacheParquet
  lazy val parquetColumn = O.apacheParquet % "parquet-column" % V.apacheParquet
  lazy val parquetCommon = O.apacheParquet % "parquet-common" % V.apacheParquet

  lazy val hadoopCommon = O.apacheHadoop % "hadoop-common" % V.apacheHadoop
  lazy val hadoopMapred = O.apacheHadoop % "hadoop-mapred" % "0.22.0"
  lazy val scalaReflect = Def.setting("org.scala-lang" % "scala-reflect" % scalaVersion.value % "provided")

  lazy val core = Seq(
    zio,
    zioSchema,
    zioSchemaDerivation,
    zioPrelude,
    parquetColumn,
    zioTest    % Test,
    zioTestSbt % Test
  )

  lazy val hadoop = Seq(
    hadoopCommon,
    hadoopMapred % Runtime,
    zio,
    zioSchema,
    zioPrelude,
    zioStreams,
    parquetHadoop,
    parquetColumn,
    parquetCommon,
    zioTest      % Test,
    zioTestSbt   % Test
  )

}
