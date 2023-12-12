package me.mnedokushev.zio.apache.parquet.core.hadoop

import me.mnedokushev.zio.apache.parquet.core.Value.GroupValue.RecordValue
import me.mnedokushev.zio.apache.parquet.core.codec.ValueDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{ ReadSupport => HadoopReadSupport }
import org.apache.parquet.hadoop.{ ParquetReader => HadoopParquetReader }
import org.apache.parquet.io.InputFile
import zio._
import zio.stream._

import scala.annotation.nowarn

trait ParquetReader[+A <: Product] {

  def readStream(path: Path): ZStream[Scope, Throwable, A]

  def readChunk(path: Path): ZIO[Scope, Throwable, Chunk[A]]

}

final class ParquetReaderLive[A <: Product](conf: Configuration)(implicit decoder: ValueDecoder[A])
    extends ParquetReader[A] {

  override def readStream(path: Path): ZStream[Scope, Throwable, A] =
    for {
      inputFile <- ZStream.fromZIO(path.toInputFileZIO(conf))
      reader    <- ZStream.fromZIO(
                     ZIO.fromAutoCloseable(
                       ZIO.attemptBlockingIO(
                         new ParquetReader.Builder(inputFile).withConf(conf).build()
                       )
                     )
                   )
      value     <- ZStream.repeatZIOOption(
                     ZIO
                       .attemptBlockingIO(reader.read())
                       .asSomeError
                       .filterOrFail(_ != null)(None)
                       .flatMap(decoder.decodeZIO(_).asSomeError)
                   )
    } yield value

  override def readChunk(path: Path): ZIO[Scope, Throwable, Chunk[A]] = {
    val builder = Chunk.newBuilder[A]

    for {
      inputFile <- path.toInputFileZIO(conf)
      reader    <- ZIO.fromAutoCloseable(
                     ZIO.attemptBlockingIO(
                       new ParquetReader.Builder(inputFile).withConf(conf).build()
                     )
                   )
      readNext   = for {
                     value  <- ZIO.attemptBlockingIO(reader.read())
                     record <- if (value != null)
                                 decoder.decodeZIO(value)
                               else
                                 ZIO.succeed(null.asInstanceOf[A])
                   } yield record
      initial   <- readNext
      _         <- {
        var current = initial

        ZIO.whileLoop(current != null)(readNext) { next =>
          builder.addOne(current)
          current = next
        }
      }
    } yield builder.result()
  }

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
