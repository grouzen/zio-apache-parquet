package me.mnedokushev.zio.apache.parquet.core.hadoop

import me.mnedokushev.zio.apache.parquet.core.Value.GroupValue.RecordValue
import me.mnedokushev.zio.apache.parquet.core.codec.ValueDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{ ReadSupport => HadoopReadSupport }
import org.apache.parquet.hadoop.{ ParquetReader => HadoopParquetReader }
import org.apache.parquet.io.InputFile
import zio._
import zio.stream._

import java.io.IOException
import scala.annotation.nowarn

trait ParquetReader[+A <: Product] {

  def readStream(path: Path): ZStream[Scope, Throwable, A]

  def readChunk(path: Path): Task[Chunk[A]]

}

final class ParquetReaderLive[A <: Product](hadoopConf: Configuration)(implicit decoder: ValueDecoder[A])
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
                       new ParquetReader.Builder(inputFile).withConf(hadoopConf).build()
                     )
                   )
    } yield reader

}

object ParquetReader {

  final class Builder(file: InputFile) extends HadoopParquetReader.Builder[RecordValue](file) {

    override protected def getReadSupport: HadoopReadSupport[RecordValue] =
      new ReadSupport

  }

  def configured[A <: Product: ValueDecoder](
    hadoopConf: Configuration = new Configuration()
  )(implicit @nowarn tag: Tag[A]): ULayer[ParquetReader[A]] =
    ZLayer.succeed(new ParquetReaderLive[A](hadoopConf))

}
