package me.mnedokushev.zio.apache.parquet.core.hadoop

import me.mnedokushev.zio.apache.parquet.core.Fixtures._
import me.mnedokushev.zio.apache.parquet.core.filter.Filter
import me.mnedokushev.zio.apache.parquet.core.filter.syntax._
import zio._
import zio.stream._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

import java.nio.file.Files

object ParquetIOSpec extends ZIOSpecDefault {

  val tmpDir     = Path(Files.createTempDirectory("zio-apache-parquet"))
  val tmpFile    = "parquet-writer-spec.parquet"
  val tmpCrcPath = tmpDir / ".parquet-writer-spec.parquet.crc"
  val tmpPath    = tmpDir / tmpFile

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
        val payload             = Chunk(
          MyRecordIO(1, "foo", None, List(1, 2), Map("first" -> 1, "second" -> 2)),
          MyRecordIO(2, "foo", None, List(1, 2), Map.empty),
          MyRecordIO(3, "bar", Some(3L), List.empty, Map("third" -> 3)),
          MyRecordIO(4, "baz", None, List.empty, Map("fourth" -> 3))
        )
        val (id, name, _, _, _) = Filter[MyRecordIO].columns

        for {
          writer <- ZIO.service[ParquetWriter[MyRecordIO]]
          reader <- ZIO.service[ParquetReader[MyRecordIO]]
          _      <- writer.writeChunk(tmpPath, payload)
          result <- reader.readChunkFiltered(tmpPath, filter(id > 1 `and` name =!= "foo"))
        } yield assertTrue(result.size == 2) && assert(result)(equalTo(payload.drop(2)))
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
