package me.mnedokushev.zio.apache.parquet.core.hadoop

import me.mnedokushev.zio.apache.parquet.core.Value
import me.mnedokushev.zio.apache.parquet.core.Value.{ GroupValue, PrimitiveValue }
import org.apache.parquet.io.api.{ Binary, Converter, GroupConverter, PrimitiveConverter }
import org.apache.parquet.schema.{ GroupType, LogicalTypeAnnotation, Type }
import zio.Chunk

import scala.jdk.CollectionConverters._

abstract class GroupValueConverter[V <: GroupValue[V]](schema: GroupType) extends GroupConverter { parent =>

  def get: V =
    this.groupValue

  def put(name: String, value: Value): Unit =
    this.groupValue = this.groupValue.put(name, value)

  protected var groupValue: V = _

  private val converters: Chunk[Converter] =
    Chunk.fromIterable(schema.getFields.asScala.toList.map(fromSchema))

  private def fromSchema(schema0: Type) = {
    val name = schema0.getName

    schema0.getLogicalTypeAnnotation match {
      case _ if schema0.isPrimitive                           =>
        primitive(name)
      case _: LogicalTypeAnnotation.ListLogicalTypeAnnotation =>
        GroupValueConverter.list(schema0.asGroupType(), name, parent)
      case _: LogicalTypeAnnotation.MapLogicalTypeAnnotation  =>
        GroupValueConverter.map(schema0.asGroupType(), name, parent)
      case _                                                  =>
        GroupValueConverter.record(schema0.asGroupType(), name, parent)
    }
  }

  override def getConverter(fieldIndex: Int): Converter =
    converters(fieldIndex)

  private def primitive(name: String) =
    new PrimitiveConverter {

      override def addBinary(value: Binary): Unit =
        parent.groupValue = parent.groupValue.put(name, PrimitiveValue.BinaryValue(value))

      override def addBoolean(value: Boolean): Unit =
        parent.groupValue = parent.groupValue.put(name, PrimitiveValue.BooleanValue(value))

      override def addDouble(value: Double): Unit =
        parent.groupValue = parent.groupValue.put(name, PrimitiveValue.DoubleValue(value))

      override def addFloat(value: Float): Unit =
        parent.groupValue = parent.groupValue.put(name, PrimitiveValue.FloatValue(value))

      override def addInt(value: Int): Unit =
        parent.groupValue = parent.groupValue.put(name, PrimitiveValue.Int32Value(value))

      override def addLong(value: Long): Unit =
        parent.groupValue = parent.groupValue.put(name, PrimitiveValue.Int64Value(value))

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

  def record[V <: GroupValue[V]](
    schema: GroupType,
    name: String,
    parent: GroupValueConverter[V]
  ): GroupValueConverter[GroupValue.RecordValue] =
    new GroupValueConverter[GroupValue.RecordValue](schema) {

      override def start(): Unit =
        this.groupValue = Value.record(Map.empty)

      override def end(): Unit =
        parent.put(name, this.groupValue)

    }

  def list[V <: GroupValue[V]](
    schema: GroupType,
    name: String,
    parent: GroupValueConverter[V]
  ): GroupValueConverter[GroupValue.ListValue] =
    new GroupValueConverter[GroupValue.ListValue](schema) {

      override def start(): Unit =
        this.groupValue = Value.list(Chunk.empty)

      override def end(): Unit =
        parent.put(name, this.groupValue)
    }

  def map[V <: GroupValue[V]](
    schema: GroupType,
    name: String,
    parent: GroupValueConverter[V]
  ): GroupValueConverter[GroupValue.MapValue] =
    new GroupValueConverter[GroupValue.MapValue](schema) {

      override def start(): Unit =
        this.groupValue = Value.map(Map.empty)

      override def end(): Unit =
        parent.put(name, this.groupValue)
    }

}
