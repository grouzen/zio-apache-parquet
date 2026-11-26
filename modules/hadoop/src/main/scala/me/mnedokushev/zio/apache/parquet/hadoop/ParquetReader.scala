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
import scala.util.control.NonFatal

trait ParquetReader[+A <: Product] {

  def readStream(path: Path): ZStream[Scope, Throwable, A]

  def readStreamFiltered(path: Path, filter: CompiledPredicate): ZStream[Scope, Throwable, A]

  def readChunk[B](path: Path): Task[Chunk[A]]

  def readChunkFiltered[B](path: Path, filter: CompiledPredicate): Task[Chunk[A]]

}

final class ParquetReaderLive[A <: Product: Tag](
  hadoopConf: Configuration,
  schema: Option[Schema[A]] = None,
  schemaEncoder: Option[SchemaEncoder[A]] = None,
  readBatchSize: Int = ParquetReader.defaultReadBatchSize
)(implicit decoder: ValueDecoder[A])
    extends ParquetReader[A] {

  require(readBatchSize > 0, "readBatchSize must be positive")

  override def readStream(path: Path): ZStream[Scope, Throwable, A] =
    for {
      reader <- ZStream.fromZIO(build(path, None))
      value  <- streamFromReader(reader)
    } yield value

  override def readStreamFiltered(path: Path, filter: CompiledPredicate): ZStream[Scope, Throwable, A] =
    for {
      reader <- ZStream.fromZIO(build(path, Some(filter)))
      value  <- streamFromReader(reader)
    } yield value

  override def readChunk[B](path: Path): Task[Chunk[A]] =
    ZIO.scoped(
      for {
        reader <- build(path, None)
        result <- streamFromReader(reader).runCollect
      } yield result
    )

  override def readChunkFiltered[B](path: Path, filter: CompiledPredicate): Task[Chunk[A]] =
    ZIO.scoped(
      for {
        reader <- build(path, Some(filter))
        result <- streamFromReader(reader).runCollect
      } yield result
    )

  private def streamFromReader(reader: HadoopParquetReader[RecordValue]): ZStream[Any, Throwable, A] =
    ZStream.suspend {
      var terminal  = Option.empty[Cause[Option[Throwable]]]
      val readBatch = ZIO.suspendSucceed {
        terminal match {
          case Some(cause) => ZIO.failCause(cause)
          case None        =>
            val batch = Chunk.newBuilder[A]
            ZIO.blocking {
              ZIO.suspendSucceed {
                var count    = 0
                var eof      = false
                var decoding = false
                try {
                  while (count < readBatchSize && !eof) {
                    val row = reader.read()
                    if (row == null) eof = true
                    else {
                      decoding = true
                      val decoded = decoder.decode(row)
                      decoding = false
                      batch.addOne(decoded)
                      count += 1
                    }
                  }
                  if (eof) Exit.fail(None) else Exit.unit
                } catch {
                  case NonFatal(error) if decoding => Exit.fail(Some(error))
                  case error: IOException          => Exit.fail(Some(error))
                  case NonFatal(error)             => Exit.die(error)
                }
              }
            }
              .as(batch.result())
              .catchAllCause { cause =>
                val rows = batch.result()
                if (rows.isEmpty || cause.isInterrupted) ZIO.failCause(cause)
                else {
                  // Emit the successful prefix before reporting EOF or failure.
                  terminal = Some(cause)
                  ZIO.succeed(rows)
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

  private[hadoop] val defaultReadBatchSize = 1024

  final class Builder[A: Tag](
    file: InputFile,
    schema: Option[Schema[A]] = None,
    schemaEncoder: Option[SchemaEncoder[A]] = None
  ) extends HadoopParquetReader.Builder[RecordValue](file) {

    override protected def getReadSupport: HadoopReadSupport[RecordValue] =
      new ReadSupport(schema, schemaEncoder)

  }

  def configured[A <: Product: ValueDecoder: Tag](
    hadoopConf: Configuration = new Configuration(),
    readBatchSize: Int = defaultReadBatchSize
  ): ULayer[ParquetReader[A]] =
    ZLayer.succeed(new ParquetReaderLive[A](hadoopConf, readBatchSize = readBatchSize))

  def projected[A <: Product: ValueDecoder: Tag](
    hadoopConf: Configuration = new Configuration(),
    readBatchSize: Int = defaultReadBatchSize
  )(implicit
    schema: Schema[A],
    schemaEncoder: SchemaEncoder[A]
  ): ULayer[ParquetReader[A]] =
    ZLayer.succeed(new ParquetReaderLive[A](hadoopConf, Some(schema), Some(schemaEncoder), readBatchSize))

}
