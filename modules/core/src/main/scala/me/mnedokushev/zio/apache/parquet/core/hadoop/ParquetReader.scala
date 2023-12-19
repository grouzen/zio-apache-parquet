package me.mnedokushev.zio.apache.parquet.core.hadoop

import me.mnedokushev.zio.apache.parquet.core.Value.GroupValue.RecordValue
import me.mnedokushev.zio.apache.parquet.core.codec.{ SchemaEncoder, ValueDecoder }
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{ ReadSupport => HadoopReadSupport }
import org.apache.parquet.hadoop.{ ParquetReader => HadoopParquetReader }
import org.apache.parquet.io.InputFile
import zio._
import zio.schema.Schema
import zio.stream._

import java.io.IOException

trait ParquetReader[+A <: Product] {

  def readStream(path: Path): ZStream[Scope, Throwable, A]

  def readChunk(path: Path): Task[Chunk[A]]

}

final class ParquetReaderLive[A <: Product: Tag](
  hadoopConf: Configuration,
  schema: Option[Schema[A]] = None,
  schemaEncoder: Option[SchemaEncoder[A]] = None
)(implicit decoder: ValueDecoder[A])
    extends ParquetReader[A] {

  override def readStream(path: Path): ZStream[Scope, Throwable, A] =
    for {
      reader <- ZStream.fromZIO(build(path))
      value  <- ZStream.repeatZIOOption(
                  ZIO
                    .attemptBlockingIO(reader.read())
                    .asSomeError
                    .filterOrFail(_ != null)(None)
                    .flatMap(decoder.decodeZIO(_).asSomeError)
                )
    } yield value

  override def readChunk(path: Path): Task[Chunk[A]] =
    ZIO.scoped(
      for {
        reader  <- build(path)
        readNext = for {
                     value  <- ZIO.attemptBlockingIO(reader.read())
                     record <- if (value != null)
                                 decoder.decodeZIO(value)
                               else
                                 ZIO.succeed(null.asInstanceOf[A])
                   } yield record
        builder  = Chunk.newBuilder[A]
        initial <- readNext
        _       <- {
          var current = initial

          ZIO.whileLoop(current != null)(readNext) { next =>
            builder.addOne(current)
            current = next
          }
        }
      } yield builder.result()
    )

  private def build(path: Path): ZIO[Scope, IOException, HadoopParquetReader[RecordValue]] =
    for {
      inputFile <- path.toInputFileZIO(hadoopConf)
      reader    <- ZIO.fromAutoCloseable(
                     ZIO.attemptBlockingIO(
                       new ParquetReader.Builder(inputFile, schema, schemaEncoder).withConf(hadoopConf).build()
                     )
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

  def configured[A <: Product: ValueDecoder](
    hadoopConf: Configuration = new Configuration()
  )(implicit tag: Tag[A]): ULayer[ParquetReader[A]] =
    ZLayer.succeed(new ParquetReaderLive[A](hadoopConf))

  def projected[A <: Product: ValueDecoder](
    hadoopConf: Configuration = new Configuration()
  )(implicit schema: Schema[A], schemaEncoder: SchemaEncoder[A], tag: Tag[A]): ULayer[ParquetReader[A]] =
    ZLayer.succeed(new ParquetReaderLive[A](hadoopConf, Some(schema), Some(schemaEncoder)))

}
