package me.mnedokushev.zio.apache.parquet.core.hadoop

import me.mnedokushev.zio.apache.parquet.core.codec._
import zio._
import zio.schema._
import zio.test._
import zio.test.TestAspect._

import java.nio.file.Files

object ParquetWriterSpec extends ZIOSpecDefault {

  val tmpDir  = Files.createTempDirectory("zio-apache-parquet")
  val tmpFile = "parquet-writer-spec.parquet"
  val tmpPath = Path(tmpDir) / tmpFile

  case class Record(a: Int, b: String)
  object Record {
    implicit val schema: Schema[Record]               =
      DeriveSchema.gen[Record]
    implicit val schemaEncoder: SchemaEncoder[Record] =
      Derive.derive[SchemaEncoder, Record](SchemaEncoderDeriver.summoned)
    implicit val valueEncoder: ValueEncoder[Record]   =
      Derive.derive[ValueEncoder, Record](ValueEncoderDeriver.summoned)
  }

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ParquetWriterSpec")(
      test("write") {
        ZIO.serviceWithZIO[ParquetWriter[Record]] { writer =>
          for {
            _ <- writer.write(Chunk(Record(1, "foo"), Record(2, "bar")))
          } yield assertTrue(true)
        }
      }.provide(ParquetWriter.configured[Record](tmpPath)) @@ after(cleanTmpFile(tmpPath))
    )

  def cleanTmpFile(path: Path) =
    ZIO.attemptBlockingIO(Files.delete(path.toJava))

}
