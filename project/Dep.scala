import sbt._

object Dep {

  object V {
    val zio                   = "2.1.1"
    val zioSchema             = "1.1.1"
    val scalaCollectionCompat = "2.12.0"
    val apacheParquet         = "1.13.1"
    val apacheHadoop          = "3.4.0"
  }

  object O {
    val apacheParquet    = "org.apache.parquet"
    val apacheHadoop     = "org.apache.hadoop"
    val scalaLang        = "org.scala-lang"
    val zio              = "dev.zio"
    val scalaLangModules = "org.scala-lang.modules"
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

  lazy val core = Seq(
    zio,
    zioSchema,
    zioSchemaDerivation,
    scalaCollectionCompat,
    parquetHadoop,
    parquetColumn,
    hadoopCommon,
    hadoopMapred,
    zioTest    % Test,
    zioTestSbt % Test
  )

}
