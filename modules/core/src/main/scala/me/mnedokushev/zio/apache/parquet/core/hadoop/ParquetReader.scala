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

  def read(path: Path): ZStream[Scope, Throwable, A]

}

final class ParquetReaderLive[A <: Product](conf: Configuration)(implicit decoder: ValueDecoder[A])
    extends ParquetReader[A] {

  override def read(path: Path): ZStream[Scope, Throwable, A] =
    for {
      inputFile <- ZStream.fromZIO(ZIO.attemptBlockingIO(path.toInputFile(conf)))
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

}

object ParquetReader {

  final class Builder(file: InputFile) extends HadoopParquetReader.Builder[RecordValue](file) {

    override def getReadSupport: HadoopReadSupport[RecordValue] =
      new ReadSupport

  }

  def configured[A <: Product: ValueDecoder](
    hadoopConf: Configuration = new Configuration()
  )(implicit @nowarn tag: Tag[A]): ULayer[ParquetReader[A]] =
    ZLayer.succeed(new ParquetReaderLive[A](hadoopConf))

}
