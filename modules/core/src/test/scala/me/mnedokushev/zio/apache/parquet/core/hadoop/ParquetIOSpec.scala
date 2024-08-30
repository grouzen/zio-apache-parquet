package me.mnedokushev.zio.apache.parquet.core.hadoop

import me.mnedokushev.zio.apache.parquet.core.Fixtures._
import me.mnedokushev.zio.apache.parquet.core.filter._
import me.mnedokushev.zio.apache.parquet.core.filter.syntax._
import zio._
import zio.stream._
import zio.test.TestAspect._
import zio.test._

import java.nio.file.Files

object ParquetIOSpec extends ZIOSpecDefault {

  val tmpDir     = Path(Files.createTempDirectory("zio-apache-parquet"))
  val tmpFile    = "parquet-writer-spec.parquet"
  val tmpCrcPath = tmpDir / ".parquet-writer-spec.parquet.crc"
  val tmpPath    = tmpDir / tmpFile

  // case class Record(a: Int, b: String, c: Option[Long], d: List[Int], e: Map[String, Int])
  // object Record {
  //   implicit val schema: Schema[Record]               =
  //     DeriveSchema.gen[Record]
  //   implicit val schemaEncoder: SchemaEncoder[Record] =
  //     Derive.derive[SchemaEncoder, Record](SchemaEncoderDeriver.summoned)
  //   implicit val valueEncoder: ValueEncoder[Record]   =
  //     Derive.derive[ValueEncoder, Record](ValueEncoderDeriver.summoned)
  //   implicit val valueDecoder: ValueDecoder[Record]   =
  //     Derive.derive[ValueDecoder, Record](ValueDecoderDeriver.summoned)
  //   implicit val typeTag: TypeTag[Record] =
  //     Derive.derive[TypeTag, Record](TypeTagDeriver.default)
  // }

  // case class ProjectedRecord(a: Int, c: Option[Long], d: List[Int], e: Map[String, Int])
  // object ProjectedRecord {
  //   implicit val schema: Schema[ProjectedRecord]               =
  //     DeriveSchema.gen[ProjectedRecord]
  //   implicit val schemaEncoder: SchemaEncoder[ProjectedRecord] =
  //     Derive.derive[SchemaEncoder, ProjectedRecord](SchemaEncoderDeriver.summoned)
  //   implicit val valueEncoder: ValueEncoder[ProjectedRecord]   =
  //     Derive.derive[ValueEncoder, ProjectedRecord](ValueEncoderDeriver.summoned)
  //   implicit val valueDecoder: ValueDecoder[ProjectedRecord]   =
  //     Derive.derive[ValueDecoder, ProjectedRecord](ValueDecoderDeriver.summoned)
  // }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("ParquetIOSpec")(
      test("write and read - chunk") {
        val payload = Chunk(
          MyRecordIO(1, "foo", None, List(1, 2), Map("first" -> 1, "second" -> 2)),
          MyRecordIO(2, "bar", Some(3L), List.empty, Map("third" -> 3))
        )

        for {
          writer <- ZIO.service[ParquetWriter[MyRecordIO]]
          reader <- ZIO.service[ParquetReader[MyRecordIO]]
          _      <- writer.writeChunk(tmpPath, payload)
          result <- reader.readChunk(tmpPath)
        } yield assertTrue(result == payload)
      } @@ after(cleanTmpFile(tmpDir)),
      test("write and read - stream") {
        val payload = Chunk(
          MyRecordIO(1, "foo", None, List(1, 2), Map("first" -> 1, "second" -> 2)),
          MyRecordIO(2, "bar", Some(3L), List.empty, Map("third" -> 3))
        )

        for {
          writer       <- ZIO.service[ParquetWriter[MyRecordIO]]
          reader       <- ZIO.service[ParquetReader[MyRecordIO]]
          _            <- writer.writeStream(tmpPath, ZStream.fromChunk(payload))
          resultStream <- ZIO.scoped[Any](reader.readStream(tmpPath).runCollect)
        } yield assertTrue(resultStream == payload)
      } @@ after(cleanTmpFile(tmpDir)),
      test("write full and read projected") {
        val payload          = Chunk(
          MyRecordIO(1, "foo", None, List(1, 2), Map("first" -> 1, "second" -> 2)),
          MyRecordIO(2, "bar", Some(3L), List.empty, Map("third" -> 3))
        )
        val projectedPayload = payload.map { r =>
          MyProjectedRecordIO(r.a, r.c, r.d, r.e)
        }

        for {
          writer <- ZIO.service[ParquetWriter[MyRecordIO]]
          reader <- ZIO.service[ParquetReader[MyProjectedRecordIO]]
          _      <- writer.writeChunk(tmpPath, payload)
          result <- reader.readChunk(tmpPath)
        } yield assertTrue(result == projectedPayload)
      } @@ after(cleanTmpFile(tmpDir)),
      test("write and read with filter") {
        val payload = Chunk(
          MyRecordIO(1, "foo", None, List(1, 2), Map("first" -> 1, "second" -> 2)),
          MyRecordIO(2, "bar", Some(3L), List.empty, Map("third" -> 3))
        )

        val (a, _, _, _, _) = Filter[MyRecordIO].columns

        for {
          writer <- ZIO.service[ParquetWriter[MyRecordIO]]
          reader <- ZIO.service[ParquetReader[MyRecordIO]]
          _      <- writer.writeChunk(tmpPath, payload)
          result <- reader.readChunk(tmpPath, filter = Some(predicate(a > 1)))
        } yield assertTrue(result.size == 1)
      } @@ after(cleanTmpFile(tmpDir))
    ).provide(
      ParquetWriter.configured[MyRecordIO](),
      ParquetReader.configured[MyRecordIO](),
      ParquetReader.projected[MyProjectedRecordIO]()
    ) @@ sequential

  private def cleanTmpFile(path: Path) =
    for {
      _ <- ZIO.attemptBlockingIO(Files.delete(tmpCrcPath.toJava))
      _ <- ZIO.attemptBlockingIO(Files.delete(tmpPath.toJava))
      _ <- ZIO.attemptBlockingIO(Files.delete(path.toJava))
    } yield ()

}
