package me.mnedokushev.zio.apache.parquet.core.hadoop

import me.mnedokushev.zio.apache.parquet.core.Value
import me.mnedokushev.zio.apache.parquet.core.Value.{ GroupValue, PrimitiveValue }
import org.apache.parquet.io.api.{ Binary, Converter, GroupConverter, PrimitiveConverter }
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{ GroupType, LogicalTypeAnnotation }
import zio.Chunk

import scala.jdk.CollectionConverters._

abstract class GroupValueConverter[V <: GroupValue[V]](
  schema: GroupType,
  parent: Option[GroupValueConverter[_]] = None
) extends GroupConverter { self =>

  def get: V =
    this.groupValue

  def put(name: String, value: Value): Unit =
    this.groupValue = this.groupValue.put(name, value)

  protected var groupValue: V = _

  private val converters: Chunk[Converter] =
    Chunk.fromIterable(
      schema.getFields.asScala.toList.map { schema0 =>
        val name = schema0.getName

        schema0.getLogicalTypeAnnotation match {
          case _ if schema0.isPrimitive                           =>
            primitive(name)
          case _: LogicalTypeAnnotation.ListLogicalTypeAnnotation =>
            list(schema0.asGroupType(), name)
          case _: LogicalTypeAnnotation.MapLogicalTypeAnnotation  =>
            map(schema0.asGroupType(), name)
          case _                                                  =>
            (name, schema0.getRepetition) match {
              case ("list", Repetition.REPEATED)      =>
                listElement(schema0.asGroupType())
              case ("key_value", Repetition.REPEATED) =>
                mapKeyValue(schema0.asGroupType(), name)
              case _                                  =>
                record(schema0.asGroupType(), name)
            }
        }
      }
    )

  override def getConverter(fieldIndex: Int): Converter =
    converters(fieldIndex)

  private def primitive(name: String) =
    new PrimitiveConverter {

      override def addBinary(value: Binary): Unit =
        parent.getOrElse(self).put(name, PrimitiveValue.BinaryValue(value))

      override def addBoolean(value: Boolean): Unit =
        parent.getOrElse(self).put(name, PrimitiveValue.BooleanValue(value))

      override def addDouble(value: Double): Unit =
        parent.getOrElse(self).put(name, PrimitiveValue.DoubleValue(value))

      override def addFloat(value: Float): Unit =
        parent.getOrElse(self).put(name, PrimitiveValue.FloatValue(value))

      override def addInt(value: Int): Unit =
        parent.getOrElse(self).put(name, PrimitiveValue.Int32Value(value))

      override def addLong(value: Long): Unit =
        parent.getOrElse(self).put(name, PrimitiveValue.Int64Value(value))

    }

  private def record(
    schema: GroupType,
    name: String
  ): GroupValueConverter[GroupValue.RecordValue] =
    new GroupValueConverter[GroupValue.RecordValue](schema, parent) {

      override def start(): Unit =
        this.groupValue = Value.record(Map.empty)

      override def end(): Unit =
        put(name, this.groupValue)

    }

  private def list(
    schema: GroupType,
    name: String
  ): GroupValueConverter[GroupValue.ListValue] =
    new GroupValueConverter[GroupValue.ListValue](schema) {

      override def start(): Unit =
        this.groupValue = Value.list(Chunk.empty)

      override def end(): Unit =
        self.put(name, this.groupValue)
    }

  private def listElement(schema: GroupType): GroupValueConverter[GroupValue.RecordValue] =
    new GroupValueConverter[GroupValue.RecordValue](schema, Some(self)) {

      override def start(): Unit = ()

      override def end(): Unit = ()

    }

  private def map(
    schema: GroupType,
    name: String
  ): GroupValueConverter[GroupValue.MapValue] =
    new GroupValueConverter[GroupValue.MapValue](schema) {

      override def start(): Unit =
        this.groupValue = Value.map(Map.empty)

      override def end(): Unit =
        self.put(name, this.groupValue)
    }

  private def mapKeyValue(
    schema: GroupType,
    name: String
  ): GroupValueConverter[GroupValue.RecordValue] =
    new GroupValueConverter[GroupValue.RecordValue](schema) {

      override def start(): Unit =
        this.groupValue = Value.record(Map("key" -> Value.nil, "value" -> Value.nil))

      override def end(): Unit =
        self.put(name, this.groupValue)

    }

}

object GroupValueConverter {

  def root(schema: GroupType): GroupValueConverter[GroupValue.RecordValue] =
    new GroupValueConverter[GroupValue.RecordValue](schema) {

      override def start(): Unit =
        this.groupValue = Value.record(
          schema.getFields.asScala.toList.map(_.getName -> Value.nil).toMap
        )

      override def end(): Unit = ()
    }

}
