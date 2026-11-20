package me.mnedokushev.zio.apache.parquet.hadoop

import me.mnedokushev.zio.apache.parquet.core.Value.GroupValue.RecordValue
import me.mnedokushev.zio.apache.parquet.core.codec.{ SchemaEncoder, ValueDecoder }
import me.mnedokushev.zio.apache.parquet.core.filter.CompiledPredicate
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.api.{ ReadSupport => HadoopReadSupport }
import org.apache.parquet.hadoop.{ ParquetReader => HadoopParquetReader }
import org.apache.parquet.io.InputFile
import zio._
import zio.schema.Schema
import zio.stream._

import java.io.IOException

trait ParquetReader[+A <: Product] {

  def readStream(path: Path): ZStream[Scope, Throwable, A]

  def readStreamFiltered(path: Path, filter: CompiledPredicate): ZStream[Scope, Throwable, A]

  def readChunk[B](path: Path): Task[Chunk[A]]

  def readChunkFiltered[B](path: Path, filter: CompiledPredicate): Task[Chunk[A]]

}

final class ParquetReaderLive[A <: Product: Tag](
  hadoopConf: Configuration,
  schema: Option[Schema[A]] = None,
  schemaEncoder: Option[SchemaEncoder[A]] = None
)(implicit decoder: ValueDecoder[A])
    extends ParquetReader[A] {

  private val readBatchSize = 1024

  override def readStream(path: Path): ZStream[Scope, Throwable, A] =
    for {
      reader <- ZStream.fromZIO(build(path, None))
      value  <- readStream0(reader)
    } yield value

  override def readStreamFiltered(path: Path, filter: CompiledPredicate): ZStream[Scope, Throwable, A] =
    for {
      reader <- ZStream.fromZIO(build(path, Some(filter)))
      value  <- readStream0(reader)
    } yield value

  override def readChunk[B](path: Path): Task[Chunk[A]] =
    ZIO.scoped(
      for {
        reader <- build(path, None)
        result <- readStream0(reader).runCollect
      } yield result
    )

  override def readChunkFiltered[B](path: Path, filter: CompiledPredicate): Task[Chunk[A]] =
    ZIO.scoped(
      for {
        reader <- build(path, Some(filter))
        result <- readStream0(reader).runCollect
      } yield result
    )

  private def readStream0(reader: HadoopParquetReader[RecordValue]): ZStream[Any, Throwable, A] =
    ZStream.suspend {
      val readNext = ZIO
        .attemptBlockingIO(reader.read())
        .asSomeError
        .filterOrFail(_ != null)(None)
        .flatMap(decoder.decodeZIO(_).asSomeError)

      var terminal  = Option.empty[Cause[Option[Throwable]]]
      val readBatch = ZIO.suspendSucceed {
        terminal match {
          case Some(cause) => ZIO.failCause(cause)
          case None        =>
            val batch = Chunk.newBuilder[A]
            var count = 0
            ZIO.blocking {
              ZIO.whileLoop(count < readBatchSize)(readNext) { row =>
                batch.addOne(row)
                count += 1
              }
            }.as(batch.result()).catchAllCause { cause =>
              if (count == 0 || cause.isInterrupted) ZIO.failCause(cause)
              else {
                // Emit the successful prefix before reporting EOF or failure.
                terminal = Some(cause)
                ZIO.succeed(batch.result())
              }
            }
        }
      }
      ZStream.repeatZIOChunkOption(readBatch)
    }

  private def build[B](
    path: Path,
    filter: Option[CompiledPredicate]
  ): ZIO[Scope, IOException, HadoopParquetReader[RecordValue]] =
    for {
      inputFile      <- path.toInputFileZIO(hadoopConf)
      compiledFilter <- ZIO.foreach(filter) { pred =>
                          ZIO
                            .fromEither(pred)
                            .mapError(new IOException(_))
                        }
      reader         <- ZIO.fromAutoCloseable(
                          ZIO.attemptBlockingIO {
                            val builder = new ParquetReader.Builder(inputFile, schema, schemaEncoder)

                            compiledFilter.foreach(pred => builder.withFilter(FilterCompat.get(pred)))
                            builder.withConf(hadoopConf).build()
                          }
                        )
    } yield reader

}

object ParquetReader {

  final class Builder[A: Tag](
    file: InputFile,
    schema: Option[Schema[A]] = None,
    schemaEncoder: Option[SchemaEncoder[A]] = None
  ) extends HadoopParquetReader.Builder[RecordValue](file) {

    override protected def getReadSupport: HadoopReadSupport[RecordValue] =
      new ReadSupport(schema, schemaEncoder)

  }

  def configured[A <: Product: ValueDecoder: Tag](
    hadoopConf: Configuration = new Configuration()
  ): ULayer[ParquetReader[A]] =
    ZLayer.succeed(new ParquetReaderLive[A](hadoopConf))

  def projected[A <: Product: ValueDecoder: Tag](
    hadoopConf: Configuration = new Configuration()
  )(implicit
    schema: Schema[A],
    schemaEncoder: SchemaEncoder[A]
  ): ULayer[ParquetReader[A]] =
    ZLayer.succeed(new ParquetReaderLive[A](hadoopConf, Some(schema), Some(schemaEncoder)))

}
