package me.mnedokushev.zio.apache.parquet.hadoop

import me.mnedokushev.zio.apache.parquet.core.Schemas
import me.mnedokushev.zio.apache.parquet.core.Value.GroupValue.RecordValue
import me.mnedokushev.zio.apache.parquet.core.codec.{ SchemaEncoder, ValueEncoder }
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{ WriteSupport => HadoopWriteSupport }
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ ParquetFileWriter, ParquetWriter => HadoopParquetWriter }
import org.apache.parquet.io.OutputFile
import org.apache.parquet.schema.MessageType
import zio._
import zio.schema.Schema
import zio.stream._

import java.io.IOException
import scala.util.control.NonFatal

trait ParquetWriter[-A <: Product] {

  def writeChunk(path: Path, data: Chunk[A]): Task[Unit]

  def writeStream[R](path: Path, data: ZStream[R, Throwable, A]): RIO[R, Unit]

}

final class ParquetWriterLive[A <: Product](
  writeMode: ParquetFileWriter.Mode,
  compressionCodecName: CompressionCodecName,
  dictionaryEncodingEnabled: Boolean,
  dictionaryPageSize: Int,
  maxPaddingSize: Int,
  pageSize: Int,
  rowGroupSize: Long,
  validationEnabled: Boolean,
  hadoopConf: Configuration,
  writeBatchSize: BatchSize.Type = ParquetWriter.defaultWriteBatchSize
)(implicit schema: Schema[A], schemaEncoder: SchemaEncoder[A], encoder: ValueEncoder[A], tag: Tag[A])
    extends ParquetWriter[A] {

  private val batchSize = BatchSize.unwrap(writeBatchSize)

  override def writeChunk(path: Path, data: Chunk[A]): Task[Unit] =
    ZIO.scoped[Any](
      for {
        writer <- build(path)
        _      <- writeBatches(writer, data)
      } yield ()
    )

  override def writeStream[R](path: Path, stream: ZStream[R, Throwable, A]): RIO[R, Unit] =
    ZIO.scoped[R](
      for {
        writer <- build(path)
        _      <- stream.runForeachChunk(writeBatches(writer, _))
      } yield ()
    )

  private def writeBatches(writer: HadoopParquetWriter[RecordValue], data: Chunk[A]): Task[Unit] =
    ZIO.suspendSucceed {
      val batches = data.grouped(batchSize)
      ZIO.whileLoop(batches.hasNext) {
        val batch = batches.next()
        ZIO.blocking {
          ZIO.suspendSucceed {
            var encoding = false
            try {
              batch.foreach { value =>
                encoding = true
                val record = encoder.encode(value)
                encoding = false
                writer.write(record.asInstanceOf[RecordValue])
              }
              Exit.unit
            } catch {
              case NonFatal(error) if encoding => Exit.fail(error)
              case error: IOException          => Exit.fail(error)
              case NonFatal(error)             => Exit.die(error)
            }
          }
        }
      }(_ => ())
    }

  private def build(path: Path): RIO[Scope, HadoopParquetWriter[RecordValue]] =
    for {
      schema        <- schemaEncoder.encodeZIO(schema, tag.tag.shortName, optional = false)
      messageSchema <- ZIO.attempt(Schemas.asMessageType(schema))
      outputFile    <- path.toOutputFileZIO(hadoopConf)
      builder        = new ParquetWriter.Builder(outputFile, messageSchema)
                         .withWriteMode(writeMode)
                         .withCompressionCodec(compressionCodecName)
                         .withDictionaryEncoding(dictionaryEncodingEnabled)
                         .withDictionaryPageSize(dictionaryPageSize)
                         .withMaxPaddingSize(maxPaddingSize)
                         .withPageSize(pageSize)
                         .withRowGroupSize(rowGroupSize)
                         .withValidation(validationEnabled)
                         .withConf(hadoopConf)
      writer        <- ZIO.fromAutoCloseable(ZIO.attemptBlockingIO(builder.build()))
    } yield writer

}

object ParquetWriter {

  private[hadoop] val defaultWriteBatchSize = BatchSize(1024)

  final class Builder(file: OutputFile, schema: MessageType)
      extends HadoopParquetWriter.Builder[RecordValue, Builder](file) {

    override def self(): Builder = this

    override def getWriteSupport(conf: Configuration): HadoopWriteSupport[RecordValue] =
      new WriteSupport(schema, Map.empty)

  }

  def configured[A <: Product: ValueEncoder](
    writeMode: ParquetFileWriter.Mode = ParquetFileWriter.Mode.CREATE,
    compressionCodecName: CompressionCodecName = HadoopParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
    dictionaryEncodingEnabled: Boolean = HadoopParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
    dictionaryPageSize: Int = HadoopParquetWriter.DEFAULT_PAGE_SIZE,
    maxPaddingSize: Int = HadoopParquetWriter.MAX_PADDING_SIZE_DEFAULT,
    pageSize: Int = HadoopParquetWriter.DEFAULT_PAGE_SIZE,
    rowGroupSize: Long = HadoopParquetWriter.DEFAULT_BLOCK_SIZE,
    validationEnabled: Boolean = HadoopParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
    hadoopConf: Configuration = new Configuration(),
    writeBatchSize: BatchSize.Type = defaultWriteBatchSize
  )(implicit
    schema: Schema[A],
    schemaEncoder: SchemaEncoder[A],
    tag: Tag[A]
  ): TaskLayer[ParquetWriter[A]] =
    ZLayer.succeed(
      new ParquetWriterLive[A](
        writeMode,
        compressionCodecName,
        dictionaryEncodingEnabled,
        dictionaryPageSize,
        maxPaddingSize,
        pageSize,
        rowGroupSize,
        validationEnabled,
        hadoopConf,
        writeBatchSize
      )
    )

}
