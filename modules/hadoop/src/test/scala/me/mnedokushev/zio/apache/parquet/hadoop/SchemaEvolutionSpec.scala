package me.mnedokushev.zio.apache.parquet.hadoop

import me.mnedokushev.zio.apache.parquet.core.Fixtures.SchemaEvolution._
import me.mnedokushev.zio.apache.parquet.core.codec.DecoderError
import zio._
import zio.test.Assertion._
import zio.test._

import java.nio.file.Files

object SchemaEvolutionSpec extends ZIOSpecDefault {

  private final case class ProjectedReader[A <: Product](reader: ParquetReader[A])

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("SchemaEvolutionSpec")(
      test("read added top-level and nested optional fields from an older file") {
        val expected = Chunk(
          NewRecord(1, None, None, NewDetails(None, 10)),
          NewRecord(2, Some("present"), None, NewDetails(None, 20))
        )

        (for {
          writer           <- ZIO.service[ParquetWriter[OldRecord]]
          configuredReader <- ZIO.service[ParquetReader[NewRecord]]
          projectedReader  <- ZIO.service[ProjectedReader[NewRecord]]
          path             <- oldFile(writer)
          configured       <- configuredReader.readChunk(path).either
          projected        <- projectedReader.reader.readChunk(path).either
        } yield assert(configured)(isRight(equalTo(expected))) &&
          assert(projected)(isRight(equalTo(expected)))).provideSome[Scope](
          ParquetWriter.configured[OldRecord](),
          ParquetReader.configured[NewRecord](),
          ParquetReader.projected[NewRecord]() >>> ZLayer.fromFunction(ProjectedReader[NewRecord](_))
        )
      },
      test("report a missing required nested field by name") {
        val missingLabel = isLeft(
          isSubtype[DecoderError](
            hasField("message", _.message, equalTo("Missing required field 'label' in record RequiredDetails"))
          )
        )

        (for {
          writer           <- ZIO.service[ParquetWriter[OldRecord]]
          configuredReader <- ZIO.service[ParquetReader[RequiredRecord]]
          projectedReader  <- ZIO.service[ProjectedReader[RequiredRecord]]
          path             <- oldFile(writer)
          configured       <- configuredReader.readChunk(path).either
          projected        <- projectedReader.reader.readChunk(path).either
        } yield assert(configured)(missingLabel) && assert(projected)(missingLabel)).provideSome[Scope](
          ParquetWriter.configured[OldRecord](),
          ParquetReader.configured[RequiredRecord](),
          ParquetReader.projected[RequiredRecord]() >>> ZLayer.fromFunction(ProjectedReader[RequiredRecord](_))
        )
      }
    )

  private def oldFile(writer: ParquetWriter[OldRecord]): ZIO[Scope, Throwable, Path] =
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
      _   <- writer.writeChunk(path, data)
    } yield path
}
