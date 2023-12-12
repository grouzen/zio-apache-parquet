package me.mnedokushev.zio.apache.parquet.core.hadoop

import me.mnedokushev.zio.apache.parquet.core.Value.GroupValue.RecordValue
import me.mnedokushev.zio.apache.parquet.core.codec.{ SchemaEncoder, ValueEncoder }
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{ WriteSupport => HadoopWriteSupport }
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.hadoop.{ ParquetFileWriter, ParquetWriter => HadoopParquetWriter }
import org.apache.parquet.io.OutputFile
import org.apache.parquet.schema.{ MessageType, Type }
import zio._
import zio.schema.Schema
import zio.stream._

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
  hadoopConf: Configuration
)(implicit schema: Schema[A], schemaEncoder: SchemaEncoder[A], encoder: ValueEncoder[A], tag: Tag[A])
    extends ParquetWriter[A] {

  override def writeChunk(path: Path, data: Chunk[A]): Task[Unit] =
    ZIO.scoped[Any](
      for {
        writer <- build(path)
        _      <- ZIO.foreachDiscard(data)(writeSingle(writer, _))
      } yield ()
    )

  override def writeStream[R](path: Path, stream: ZStream[R, Throwable, A]): RIO[R, Unit] =
    ZIO.scoped[R](
      for {
        writer <- build(path)
        _      <- stream.runForeach(writeSingle(writer, _))
      } yield ()
    )

  private def writeSingle(writer: HadoopParquetWriter[RecordValue], value: A) =
    for {
      record <- encoder.encodeZIO(value)
      _      <- ZIO.attemptBlockingIO(writer.write(record.asInstanceOf[RecordValue]))
    } yield ()

  private def build(path: Path) = {

    def castToMessageSchema(schema: Type) =
      ZIO.attempt {
        val groupSchema = schema.asGroupType()
        val name        = groupSchema.getName
        val fields      = groupSchema.getFields

        new MessageType(name, fields)
      }

    for {
      schema        <- schemaEncoder.encodeZIO(schema, tag.tag.shortName, optional = false)
      messageSchema <- castToMessageSchema(schema)
      hadoopFile    <- ZIO.attemptBlockingIO(HadoopOutputFile.fromPath(path.toHadoop, hadoopConf))
      builder        = new ParquetWriter.Builder(hadoopFile, messageSchema)
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

}

object ParquetWriter {

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
    hadoopConf: Configuration = new Configuration()
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
        hadoopConf
      )
    )

}
