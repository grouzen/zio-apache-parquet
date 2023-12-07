package me.mnedokushev.zio.apache.parquet.core.hadoop

import me.mnedokushev.zio.apache.parquet.core.Value.GroupValue.RecordValue
import me.mnedokushev.zio.apache.parquet.core.codec.{ SchemaEncoder, ValueEncoder }
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.{ ParquetFileWriter, ParquetWriter => HadoopParquetWriter }
import org.apache.parquet.hadoop.api.{ WriteSupport => HadoopWriteSupport }
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.io.OutputFile
import org.apache.parquet.schema.{ MessageType, Type }
import zio._
import zio.schema.Schema

trait ParquetWriter[A <: Product] {

  def write(data: Chunk[A]): Task[Unit]

}

final class ParquetWriterLive[A <: Product](
  underlying: HadoopParquetWriter[RecordValue]
)(implicit encoder: ValueEncoder[A])
    extends ParquetWriter[A] {

  override def write(data: Chunk[A]): Task[Unit] =
    ZIO.attemptBlocking(
      data.foreach(v => underlying.write(encoder.encode(v).asInstanceOf[RecordValue]))
    )

}

object ParquetWriter {

  final private class Builder(file: OutputFile, schema: MessageType)
      extends HadoopParquetWriter.Builder[RecordValue, Builder](file) {

    override def self(): Builder = this

    override def getWriteSupport(conf: Configuration): HadoopWriteSupport[RecordValue] =
      new WriteSupport(schema, Map.empty)

  }

  def configured[A <: Product](
    path: Path,
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
    encoder: ValueEncoder[A],
    tag: Tag[A]
  ): TaskLayer[ParquetWriter[A]] = {

    def castSchema(schema: Type) =
      ZIO.attempt {
        val groupSchema = schema.asGroupType()
        val name        = groupSchema.getName
        val fields      = groupSchema.getFields

        new MessageType(name, fields)
      }

    ZLayer.scoped(
      for {
        schema        <- schemaEncoder.encodeZIO(schema, tag.tag.shortName, optional = false)
        messageSchema <- castSchema(schema)
        hadoopFile    <- ZIO.attemptBlockingIO(HadoopOutputFile.fromPath(path.underlying, hadoopConf))
        builder        = new Builder(hadoopFile, messageSchema)
                           .withWriteMode(writeMode)
                           .withCompressionCodec(compressionCodecName)
                           .withDictionaryEncoding(dictionaryEncodingEnabled)
                           .withDictionaryPageSize(dictionaryPageSize)
                           .withMaxPaddingSize(maxPaddingSize)
                           .withPageSize(pageSize)
                           .withRowGroupSize(rowGroupSize)
                           .withValidation(validationEnabled)
                           .withConf(hadoopConf)
        underlying    <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(builder.build()))
        writer         = new ParquetWriterLive[A](underlying)
      } yield writer
    )
  }

}
