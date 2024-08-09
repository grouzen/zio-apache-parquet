package me.mnedokushev.zio.apache.parquet.core.hadoop

import me.mnedokushev.zio.apache.parquet.core.Value.GroupValue.RecordValue
import me.mnedokushev.zio.apache.parquet.core.Value.NullValue
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{ WriteSupport => HadoopWriteSupport }
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType

import scala.jdk.CollectionConverters._

class WriteSupport(schema: MessageType, metadata: Map[String, String]) extends HadoopWriteSupport[RecordValue] {

  override def init(configuration: Configuration): HadoopWriteSupport.WriteContext =
    new HadoopWriteSupport.WriteContext(schema, metadata.asJava)

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit =
    this.consumer = recordConsumer

  override def write(record: RecordValue): Unit = {
    consumer.startMessage()

    record.values.foreach {
      case (_, NullValue) =>
        ()
      case (name, value)  =>
        val fieldIndex = schema.getFieldIndex(name)
        val fieldType  = schema.getType(fieldIndex)

        consumer.startField(name, fieldIndex)
        value.write(fieldType, consumer)
        consumer.endField(name, fieldIndex)
    }

    consumer.endMessage()
  }

  private var consumer: RecordConsumer = null

}
