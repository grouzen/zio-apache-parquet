package me.mnedokushev.zio.apache.parquet.core

import org.apache.parquet.io.api.{ Binary, RecordConsumer }
import org.apache.parquet.schema.Type
import zio.Chunk

trait Value {
  def write(schema: Type, recordConsumer: RecordConsumer): Unit
}

trait PrimitiveValue[A] extends Value {
  def value: A
}

case class BooleanValue(value: Boolean) extends PrimitiveValue[Boolean] {

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit =
    recordConsumer.addBoolean(value)

}

case class Int32Value(value: Int) extends PrimitiveValue[Int] {

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit =
    recordConsumer.addInteger(value)

}

case class Int64Value(value: Long) extends PrimitiveValue[Long] {

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit =
    recordConsumer.addLong(value)

}

case class FloatValue(value: Float) extends PrimitiveValue[Float] {

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit =
    recordConsumer.addFloat(value)

}

case class DoubleValue(value: Double) extends PrimitiveValue[Double] {

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit =
    recordConsumer.addDouble(value)

}

case class ByteArrayValue(value: Binary) extends PrimitiveValue[Binary] {

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit =
    recordConsumer.addBinary(value)

}

trait GroupValue extends Value

case class RecordValue(values: Map[String, Value]) extends GroupValue {

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = {
    val groupSchema = schema.asGroupType()

    recordConsumer.startGroup()

    values.foreach { case (name, value) =>
      val fieldIndex = groupSchema.getFieldIndex(name)

      recordConsumer.startField(name, fieldIndex)
      value.write(groupSchema.getType(name), recordConsumer)
      recordConsumer.endField(name, fieldIndex)
    }

    recordConsumer.endGroup()
  }

}

case class ListValue(values: Chunk[Value]) extends GroupValue {

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = {
    recordConsumer.startGroup()

    if (values.nonEmpty) {
      val groupSchema   = schema.asGroupType()
      val listSchema    = groupSchema.getFields.get(0).asGroupType()
      val listFieldName = listSchema.getName
      val elementName   = listSchema.getFields.get(0).getName
      val listIndex     = groupSchema.getFieldIndex(listFieldName)

      recordConsumer.startField(listFieldName, listIndex)

      values.foreach { value =>
        RecordValue(Map(elementName -> value)).write(listSchema, recordConsumer)
      }

      recordConsumer.endField(listFieldName, listIndex)
    }

    recordConsumer.endGroup()
  }

}

case class MapValue(values: Map[Value, Value]) extends GroupValue {

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = {
    recordConsumer.startGroup()

    if (values.nonEmpty) {
      val groupSchema  = schema.asGroupType()
      val mapSchema    = groupSchema.getFields.get(0).asGroupType()
      val mapFieldName = mapSchema.getName
      val mapIndex     = groupSchema.getFieldIndex(mapFieldName)

      recordConsumer.startField(mapFieldName, mapIndex)

      values.foreach { case (key, value) =>
        RecordValue(Map("key" -> key, "value" -> value)).write(mapSchema, recordConsumer)
      }

      recordConsumer.endField(mapFieldName, mapIndex)
    }

    recordConsumer.endGroup()
  }

}
