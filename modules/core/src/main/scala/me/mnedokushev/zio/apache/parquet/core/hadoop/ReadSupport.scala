package me.mnedokushev.zio.apache.parquet.core.hadoop

import me.mnedokushev.zio.apache.parquet.core.Schemas
import me.mnedokushev.zio.apache.parquet.core.Value.GroupValue.RecordValue
import me.mnedokushev.zio.apache.parquet.core.codec.SchemaEncoder
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{ InitContext, ReadSupport => HadoopReadSupport }
import org.apache.parquet.io.api.{ GroupConverter, RecordMaterializer }
import org.apache.parquet.schema.MessageType
import zio.prelude._
import zio.schema.Schema
import zio.Tag

class ReadSupport[A](
  schema: Option[Schema[A]] = None,
  schemaEncoder: Option[SchemaEncoder[A]] = None
)(implicit tag: Tag[A])
    extends HadoopReadSupport[RecordValue] {

  override def prepareForRead(
    configuration: Configuration,
    keyValueMetaData: java.util.Map[String, String],
    fileSchema: MessageType,
    readContext: HadoopReadSupport.ReadContext
  ): RecordMaterializer[RecordValue] = new RecordMaterializer[RecordValue] {

    private val converter =
      GroupValueConverter.root(resolveSchema(fileSchema))

    override def getCurrentRecord: RecordValue =
      converter.get

    override def getRootConverter: GroupConverter =
      converter

  }

  override def init(context: InitContext): HadoopReadSupport.ReadContext =
    new HadoopReadSupport.ReadContext(resolveSchema(context.getFileSchema))

  private def resolveSchema(contextSchema: MessageType): MessageType =
    (schema <*> schemaEncoder).fold(contextSchema) { case (schema0, schemaEncoder0) =>
      Schemas.asMessageType(schemaEncoder0.encode(schema0, tag.tag.shortName, optional = false))
    }

}
