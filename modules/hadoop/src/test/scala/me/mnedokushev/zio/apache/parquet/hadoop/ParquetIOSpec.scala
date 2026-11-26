package me.mnedokushev.zio.apache.parquet.hadoop

import me.mnedokushev.zio.apache.parquet.core.Fixtures._
import me.mnedokushev.zio.apache.parquet.core.codec.{ ValueDecoder, ValueEncoder }
import me.mnedokushev.zio.apache.parquet.core.filter.Filter
import me.mnedokushev.zio.apache.parquet.core.filter.syntax._
import org.apache.hadoop.conf.Configuration
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
        val payload = Chunk.fromIterable((1 to 1025).map { id =>
          MyRecordIO(id, "foo", None, List(1, 2), Map("first" -> 1, "second" -> 2))
        })

        for {
          writer <- ZIO.service[ParquetWriter[MyRecordIO]]
          reader <- ZIO.service[ParquetReader[MyRecordIO]]
          _      <- writer.writeChunk(tmpPath, payload)
          result <- reader.readChunk(tmpPath)
        } yield assertTrue(result == payload)
      } @@ after(cleanTmpFile(tmpDir)),
      test("write and read - stream") {
        val first = Chunk.single(MyRecordIO(0, "bar", Some(3L), List.empty, Map("third" -> 3)))
        val rest  = Chunk.fromIterable((1 to 1025).map { id =>
          MyRecordIO(id, "foo", None, List(1, 2), Map("first" -> 1, "second" -> 2))
        })

        for {
          writer  <- ZIO.service[ParquetWriter[MyRecordIO]]
          reader  <- ZIO.service[ParquetReader[MyRecordIO]]
          _       <- writer.writeStream(tmpPath, ZStream.fromChunks(first, rest))
          batches <- ZIO.scoped[Any](reader.readStream(tmpPath).chunks.runCollect)
          custom  <- ZIO.scoped {
                       for {
                         env  <- ParquetReader.configured[MyRecordIO](readBatchSize = BatchSize(512)).build
                         rows <- env.get.readStream(tmpPath).chunks.runCollect
                       } yield rows
                     }
        } yield assertTrue(
          batches.map(_.size) == Chunk(1024, 2),
          batches.flatten == first ++ rest,
          custom.map(_.size) == Chunk(512, 512, 2),
          custom.flatten == batches.flatten
        )
      } @@ after(cleanTmpFile(tmpDir)),
      test("write stops at an encoding failure and closes the file") {
        val first                                             = MyRecordIO(1, "foo", None, List.empty, Map.empty)
        val failure                                           = new IllegalStateException("failed to encode row 2")
        implicit val failingEncoder: ValueEncoder[MyRecordIO] = MyRecordIO.valueEncoder.contramap[MyRecordIO] { row =>
          if (row.a == 2) throw failure
          row
        }

        for {
          writer <- ZIO.scoped(ParquetWriter.configured[MyRecordIO](writeBatchSize = BatchSize(2)).build.map(_.get))
          reader <- ZIO.service[ParquetReader[MyRecordIO]]
          result <- writer.writeChunk(tmpPath, Chunk(first, first.copy(a = 2), first.copy(a = 3))).either
          rows   <- reader.readChunk(tmpPath)
        } yield assertTrue(result == Left(failure), rows == Chunk(first))
      } @@ after(cleanTmpFile(tmpDir)),
      test("stream emits rows before a decoding failure within a batch") {
        val first                                             = MyRecordIO(1, "foo", None, List.empty, Map.empty)
        val failure                                           = new IllegalStateException("failed to decode row 2")
        implicit val failingDecoder: ValueDecoder[MyRecordIO] = MyRecordIO.valueDecoder.map { row =>
          if (row.a == 2) throw failure
          row
        }
        val reader                                            = new ParquetReaderLive[MyRecordIO](new Configuration())

        for {
          writer <- ZIO.service[ParquetWriter[MyRecordIO]]
          _      <- writer.writeChunk(tmpPath, Chunk(first, first.copy(a = 2)))
          prefix <- ZIO.scoped(reader.readStream(tmpPath).take(1).runCollect)
          seen   <- Ref.make(Chunk.empty[MyRecordIO])
          result <- ZIO.scoped(reader.readStream(tmpPath).runForeach(row => seen.update(_ :+ row))).either
          rows   <- seen.get
        } yield assertTrue(prefix == Chunk(first), rows == prefix, result == Left(failure))
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
