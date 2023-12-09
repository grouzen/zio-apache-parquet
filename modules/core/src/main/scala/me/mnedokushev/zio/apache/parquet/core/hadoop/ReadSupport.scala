package me.mnedokushev.zio.apache.parquet.core.hadoop

import me.mnedokushev.zio.apache.parquet.core.Value.GroupValue.RecordValue
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{ InitContext, ReadSupport => HadoopReadSupport }
import org.apache.parquet.io.api.{ GroupConverter, RecordMaterializer }
import org.apache.parquet.schema.MessageType

import java.util

class ReadSupport extends HadoopReadSupport[RecordValue] {

  override def prepareForRead(
    configuration: Configuration,
    keyValueMetaData: util.Map[String, String],
    fileSchema: MessageType,
    readContext: HadoopReadSupport.ReadContext
  ): RecordMaterializer[RecordValue] = new RecordMaterializer[RecordValue] {

    private val converter =
      GroupValueConverter.root(fileSchema)

    override def getCurrentRecord: RecordValue =
      converter.get

    override def getRootConverter: GroupConverter =
      converter

  }

  override def init(context: InitContext): HadoopReadSupport.ReadContext =
    new HadoopReadSupport.ReadContext(context.getFileSchema)

}
