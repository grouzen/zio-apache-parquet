package me.mnedokushev.zio.apache.parquet.hadoop

import me.mnedokushev.zio.apache.parquet.core.Schemas
import me.mnedokushev.zio.apache.parquet.core.Value.GroupValue.RecordValue
import me.mnedokushev.zio.apache.parquet.core.codec.SchemaEncoder
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{ InitContext, ReadSupport => HadoopReadSupport }
import org.apache.parquet.io.api.{ GroupConverter, RecordMaterializer }
import org.apache.parquet.schema.{ GroupType, MessageType, Type }
import zio.Tag
import zio.prelude._
import zio.schema.Schema

import scala.jdk.CollectionConverters._

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
      GroupValueConverter.root(readContext.getRequestedSchema)

    override def getCurrentRecord: RecordValue =
      converter.get

    override def getRootConverter: GroupConverter =
      converter

  }

  override def init(context: InitContext): HadoopReadSupport.ReadContext = {
    val fileSchema      = context.getFileSchema
    val requestedSchema = (schema <*> schemaEncoder).fold(fileSchema) { case (schema0, schemaEncoder0) =>
      val projection = Schemas.asMessageType(schemaEncoder0.encode(schema0, tag.tag.shortName, optional = false))
      new MessageType(projection.getName, projectFields(projection, fileSchema).asJava)
    }
    new HadoopReadSupport.ReadContext(requestedSchema)
  }

  // Keep absent columns out of the materializer so only columns in the file can be initialized to null.
  private def projectFields(projection: GroupType, file: GroupType): List[Type] =
    projection.getFields.asScala.toList.filter(field => file.containsField(field.getName)).map { field =>
      val fileField = file.getType(field.getName)
      if (field.isPrimitive || fileField.isPrimitive) field
      else field.asGroupType().withNewFields(projectFields(field.asGroupType(), fileField.asGroupType()).asJava)
    }

}
