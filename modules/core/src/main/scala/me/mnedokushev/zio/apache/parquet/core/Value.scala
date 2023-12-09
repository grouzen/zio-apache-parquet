package me.mnedokushev.zio.apache.parquet.core

import org.apache.parquet.io.api.{ Binary, RecordConsumer }
import org.apache.parquet.schema.Type
import zio.Chunk

import java.nio.ByteBuffer
import java.util.UUID

sealed trait Value {
  def write(schema: Type, recordConsumer: RecordConsumer): Unit
}

object Value {

  case object NullValue extends Value {
    override def write(schema: Type, recordConsumer: RecordConsumer): Unit =
      throw new UnsupportedOperationException(s"NullValue cannot be written")
  }

  sealed trait PrimitiveValue[A] extends Value {
    def value: A
  }

  object PrimitiveValue {

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

    case class BinaryValue(value: Binary) extends PrimitiveValue[Binary] {

      override def write(schema: Type, recordConsumer: RecordConsumer): Unit =
        recordConsumer.addBinary(value)

    }

  }

  sealed trait GroupValue[Self <: GroupValue[Self]] extends Value {

    def put(name: String, value: Value): Self

  }

  object GroupValue {

    case class RecordValue(values: Map[String, Value]) extends GroupValue[RecordValue] {

      override def write(schema: Type, recordConsumer: RecordConsumer): Unit = {
        val groupSchema = schema.asGroupType()

        recordConsumer.startGroup()

        values.foreach { case (name, value) =>
          val fieldIndex = groupSchema.getFieldIndex(name)
          val fieldType  = groupSchema.getType(name)

          recordConsumer.startField(name, fieldIndex)
          value.write(fieldType, recordConsumer)
          recordConsumer.endField(name, fieldIndex)
        }

        recordConsumer.endGroup()
      }

      override def put(name: String, value: Value): RecordValue =
        if (values.contains(name))
          this.copy(values.updated(name, value))
        else
          throw new IllegalArgumentException(s"Record doesn't contain field $name")

    }

    case class ListValue(values: Chunk[Value]) extends GroupValue[ListValue] {

      override def write(schema: Type, recordConsumer: RecordConsumer): Unit = {
        recordConsumer.startGroup()

        if (values.nonEmpty) {
          val groupSchema   = schema.asGroupType()
          val listSchema    = groupSchema.getFields.get(0).asGroupType()
          val listFieldName = listSchema.getName
          val elementName   = listSchema.getFields.get(0).getName // TODO: validate, must be "element"
          val listIndex     = groupSchema.getFieldIndex(listFieldName)

          recordConsumer.startField(listFieldName, listIndex)

          values.foreach { value =>
            RecordValue(Map(elementName -> value)).write(listSchema, recordConsumer)
          }

          recordConsumer.endField(listFieldName, listIndex)
        }

        recordConsumer.endGroup()
      }

      override def put(name: String, value: Value): ListValue =
        this.copy(values = values :+ value)

    }

    case class MapValue(values: Map[Value, Value]) extends GroupValue[MapValue] {

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

      override def put(name: String, value: Value): MapValue = ???
//        this.copy(values = values.updated(name, value))
    }

  }

  def nil =
    NullValue

  def string(v: String) =
    PrimitiveValue.BinaryValue(Binary.fromString(v))

  def boolean(v: Boolean) =
    PrimitiveValue.BooleanValue(v)

  def short(v: Short) =
    PrimitiveValue.Int32Value(v.toInt)

  def int(v: Int) =
    PrimitiveValue.Int32Value(v)

  def long(v: Long) =
    PrimitiveValue.Int64Value(v)

  def float(v: Float) =
    PrimitiveValue.FloatValue(v)

  def double(v: Double) =
    PrimitiveValue.DoubleValue(v)

  def binary(v: Chunk[Byte]) =
    PrimitiveValue.BinaryValue(Binary.fromConstantByteArray(v.toArray))

  def char(v: Char) =
    PrimitiveValue.Int32Value(v.toInt)

  def uuid(v: UUID) = {
    val bb = ByteBuffer.wrap(Array.ofDim(16))

    bb.putLong(v.getMostSignificantBits)
    bb.putLong(v.getLeastSignificantBits)

    PrimitiveValue.BinaryValue(Binary.fromConstantByteArray(bb.array()))
  }

  def record(r: Map[String, Value]) =
    GroupValue.RecordValue(r)

  def list(vs: Chunk[Value]) =
    GroupValue.ListValue(vs)

  def map(kvs: Map[Value, Value]) =
    GroupValue.MapValue(kvs)
}
