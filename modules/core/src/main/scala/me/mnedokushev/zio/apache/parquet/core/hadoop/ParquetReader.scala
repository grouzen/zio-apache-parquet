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
import me.mnedokushev.zio.apache.parquet.core.filter.Expr
import org.apache.parquet.filter2.compat.FilterCompat

trait ParquetReader[+A <: Product] {

  def readStream(path: Path): ZStream[Scope, Throwable, A]

  def readChunk[B](path: Path, filter: Option[Expr.Predicate[B]] = None): Task[Chunk[A]]

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

  override def readChunk[B](path: Path, filter: Option[Expr.Predicate[B]] = None): Task[Chunk[A]] =
    ZIO.scoped(
      for {
        reader  <- build(path, filter)
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

  private def build[B](
    path: Path,
    filter: Option[Expr.Predicate[B]] = None
  ): ZIO[Scope, IOException, HadoopParquetReader[RecordValue]] =
    for {
      inputFile      <- path.toInputFileZIO(hadoopConf)
      compiledFilter <- ZIO.foreach(filter) { pred =>
                          ZIO
                            .fromEither(Expr.compile(pred))
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
