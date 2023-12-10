package me.mnedokushev.zio.apache.parquet.core.hadoop

import me.mnedokushev.zio.apache.parquet.core.codec._
import zio._
import zio.schema._
import zio.test.TestAspect._
import zio.test._

import java.nio.file.Files

object ParquetIOSpec extends ZIOSpecDefault {

  val tmpDir     = Path(Files.createTempDirectory("zio-apache-parquet"))
  val tmpFile    = "parquet-writer-spec.parquet"
  val tmpCrcPath = tmpDir / ".parquet-writer-spec.parquet.crc"
  val tmpPath    = tmpDir / tmpFile

  case class Record(a: Int, b: String, c: Option[Long])
  object Record {
    implicit val schema: Schema[Record]               =
      DeriveSchema.gen[Record]
    implicit val schemaEncoder: SchemaEncoder[Record] =
      Derive.derive[SchemaEncoder, Record](SchemaEncoderDeriver.summoned)
    implicit val valueEncoder: ValueEncoder[Record]   =
      Derive.derive[ValueEncoder, Record](ValueEncoderDeriver.summoned)
    implicit val valueDecoder: ValueDecoder[Record]   =
      Derive.derive[ValueDecoder, Record](ValueDecoderDeriver.summoned)
  }

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ParquetIOSpec")(
      test("write and read") {
        val payload = Chunk(Record(1, "foo", None), Record(2, "bar", Some(3L)))

        for {
          writer <- ZIO.service[ParquetWriter[Record]]
          reader <- ZIO.service[ParquetReader[Record]]
          _      <- writer.write(payload)
          _      <- writer.close // force to flush parquet data on disk
          result <- ZIO.scoped[Any](reader.read(tmpPath).runCollect)
        } yield assertTrue(result == payload)
      }.provide(
        ParquetWriter.configured[Record](tmpPath),
        ParquetReader.configured[Record]()
      ) @@ after(cleanTmpFile(tmpDir))
    )

  def cleanTmpFile(path: Path) =
    for {
      _ <- ZIO.attemptBlockingIO(Files.delete(tmpCrcPath.toJava))
      _ <- ZIO.attemptBlockingIO(Files.delete(tmpPath.toJava))
      _ <- ZIO.attemptBlockingIO(Files.delete(path.toJava))
    } yield ()

}
