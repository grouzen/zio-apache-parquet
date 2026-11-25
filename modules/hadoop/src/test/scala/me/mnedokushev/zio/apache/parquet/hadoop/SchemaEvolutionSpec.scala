package me.mnedokushev.zio.apache.parquet.hadoop

import me.mnedokushev.zio.apache.parquet.core.Fixtures.SchemaEvolution._
import me.mnedokushev.zio.apache.parquet.core.codec.DecoderError
import zio._
import zio.test.Assertion._
import zio.test._

import java.nio.file.Files

object SchemaEvolutionSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("SchemaEvolutionSpec")(
      test("read added top-level and nested optional fields from an older file") {
        val expected = Chunk(
          NewRecord(1, None, None, NewDetails(None, 10)),
          NewRecord(2, Some("present"), None, NewDetails(None, 20))
        )

        for {
          path       <- oldFile
          configured <- read[NewRecord](path).provide(ParquetReader.configured[NewRecord]()).either
          projected  <- read[NewRecord](path).provide(ParquetReader.projected[NewRecord]()).either
        } yield assert(configured)(isRight(equalTo(expected))) &&
          assert(projected)(isRight(equalTo(expected)))
      },
      test("report a missing required nested field by name") {
        val missingLabel = isLeft(isSubtype[DecoderError](hasField("message", _.message, containsString("'label'"))))

        for {
          path       <- oldFile
          configured <- read[RequiredRecord](path).provide(ParquetReader.configured[RequiredRecord]()).either
          projected  <- read[RequiredRecord](path).provide(ParquetReader.projected[RequiredRecord]()).either
        } yield assert(configured)(missingLabel) && assert(projected)(missingLabel)
      }
    )

  private def read[A <: Product: Tag](path: Path): ZIO[ParquetReader[A], Throwable, Chunk[A]] =
    ZIO.serviceWithZIO[ParquetReader[A]](_.readChunk(path))

  private def oldFile: ZIO[Scope, Throwable, Path] =
    for {
      dir <- ZIO.acquireRelease(ZIO.attemptBlockingIO(Path(Files.createTempDirectory("parquet-evolution")))) { dir =>
               ZIO.attemptBlockingIO {
                 Files.deleteIfExists((dir / ".old.parquet.crc").toJava)
                 Files.deleteIfExists((dir / "old.parquet").toJava)
                 Files.delete(dir.toJava)
               }.orDie
             }
      path = dir / "old.parquet"
      data = Chunk(
               OldRecord(1, None, "ignored", OldDetails(10)),
               OldRecord(2, Some("present"), "ignored", OldDetails(20))
             )
      _   <- ZIO
               .serviceWithZIO[ParquetWriter[OldRecord]](_.writeChunk(path, data))
               .provide(ParquetWriter.configured[OldRecord]())
    } yield path
}
