package me.mnedokushev.zio.apache.parquet.hadoop

import me.mnedokushev.zio.apache.parquet.core.Value
import me.mnedokushev.zio.apache.parquet.core.Value.{ GroupValue, PrimitiveValue }
import org.apache.parquet.io.api.{ Binary, Converter, GroupConverter, PrimitiveConverter }
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{ GroupType, LogicalTypeAnnotation }
import zio.Chunk

import scala.jdk.CollectionConverters._

trait GroupValueConverter[V <: GroupValue[V]] extends GroupConverter {

  def get: V =
    this.groupValue

  def put(name: String, value: Value): Unit

  protected var groupValue: V =
    null.asInstanceOf[V]

  protected val converters: Chunk[Converter]

  override def getConverter(fieldIndex: Int): Converter =
    converters(fieldIndex)

}

object GroupValueConverter {

  abstract case class Default[V <: GroupValue[V]](schema: GroupType) extends GroupValueConverter[V] {

    override def put(name: String, value: Value): Unit =
      this.groupValue = this.groupValue.put(name, value)

    override protected val converters: Chunk[Converter] =
      Chunk.fromIterable(
        schema.getFields.asScala.toList.map { schema0 =>
          val name = schema0.getName

          schema0.getLogicalTypeAnnotation match {
            case _ if schema0.isPrimitive                           =>
              GroupValueConverter.primitive(name, this)
            case _: LogicalTypeAnnotation.ListLogicalTypeAnnotation =>
              GroupValueConverter.list(schema0.asGroupType(), name, this)
            case _: LogicalTypeAnnotation.MapLogicalTypeAnnotation  =>
              GroupValueConverter.map(schema0.asGroupType(), name, this)
            case _                                                  =>
              (name, schema0.getRepetition) match {
                case ("list", Repetition.REPEATED)      =>
                  GroupValueConverter.listElement(schema0.asGroupType(), this)
                case ("key_value", Repetition.REPEATED) =>
                  GroupValueConverter.mapKeyValue(schema0.asGroupType(), name, this)
                case _                                  =>
                  GroupValueConverter.record(schema0.asGroupType(), name, this)
              }
          }
        }
      )

  }

  abstract case class ByPass[V <: GroupValue[V], S <: GroupValue[S]](
    schema: GroupType,
    toSelf: GroupValueConverter[S]
  ) extends GroupValueConverter[V] {

    override def put(name: String, value: Value): Unit =
      toSelf.groupValue = toSelf.groupValue.put(name, value)

    override protected val converters: Chunk[Converter] =
      Chunk.fromIterable(
        schema.getFields.asScala.toList.map { schema0 =>
          val name = schema0.getName

          schema0.getLogicalTypeAnnotation match {
            case _ if schema0.isPrimitive                           =>
              GroupValueConverter.primitive(name, toSelf)
            case _: LogicalTypeAnnotation.ListLogicalTypeAnnotation =>
              GroupValueConverter.list(schema0.asGroupType(), name, this)
            case _: LogicalTypeAnnotation.MapLogicalTypeAnnotation  =>
              GroupValueConverter.map(schema0.asGroupType(), name, this)
            case _                                                  =>
              (name, schema0.getRepetition) match {
                case ("list", Repetition.REPEATED)      =>
                  GroupValueConverter.listElement(schema0.asGroupType(), this)
                case ("key_value", Repetition.REPEATED) =>
                  GroupValueConverter.mapKeyValue(schema0.asGroupType(), name, this)
                case _                                  =>
                  GroupValueConverter.record(schema0.asGroupType(), name, this)
              }
          }
        }
      )

  }

  def primitive[V <: GroupValue[V]](name: String, parent: GroupValueConverter[V]): PrimitiveConverter =
    new PrimitiveConverter {

      override def addBinary(value: Binary): Unit =
        parent.put(name, PrimitiveValue.BinaryValue(value))

      override def addBoolean(value: Boolean): Unit =
        parent.put(name, PrimitiveValue.BooleanValue(value))

      override def addDouble(value: Double): Unit =
        parent.put(name, PrimitiveValue.DoubleValue(value))

      override def addFloat(value: Float): Unit =
        parent.put(name, PrimitiveValue.FloatValue(value))

      override def addInt(value: Int): Unit =
        parent.put(name, PrimitiveValue.Int32Value(value))

      override def addLong(value: Long): Unit =
        parent.put(name, PrimitiveValue.Int64Value(value))

    }

  def record[V <: GroupValue[V]](
    schema: GroupType,
    name: String,
    parent: GroupValueConverter[V]
  ): GroupValueConverter[GroupValue.RecordValue] =
    new Default[GroupValue.RecordValue](schema) {

      override def start(): Unit =
        this.groupValue = Value.record(
          this.schema.getFields.asScala.toList.map(_.getName -> Value.nil).toMap
        )

      override def end(): Unit =
        parent.put(name, this.groupValue)

    }

  def list[V <: GroupValue[V]](
    schema: GroupType,
    name: String,
    parent: GroupValueConverter[V]
  ): GroupValueConverter[GroupValue.ListValue] =
    new Default[GroupValue.ListValue](schema) {

      override def start(): Unit =
        this.groupValue = Value.list(Chunk.empty)

      override def end(): Unit =
        parent.put(name, this.groupValue)
    }

  def listElement[V <: GroupValue[V], S <: GroupValue[S]](
    schema: GroupType,
    parent: GroupValueConverter[S]
  ): GroupValueConverter[GroupValue.RecordValue] =
    new ByPass[GroupValue.RecordValue, S](schema, parent) {

      override def start(): Unit = ()

      override def end(): Unit = ()

    }

  def map[V <: GroupValue[V]](
    schema: GroupType,
    name: String,
    parent: GroupValueConverter[V]
  ): GroupValueConverter[GroupValue.MapValue] =
    new Default[GroupValue.MapValue](schema) {

      override def start(): Unit =
        this.groupValue = Value.map(Map.empty)

      override def end(): Unit =
        parent.put(name, this.groupValue)
    }

  def mapKeyValue[V <: GroupValue[V]](
    schema: GroupType,
    name: String,
    parent: GroupValueConverter[V]
  ): GroupValueConverter[GroupValue.RecordValue] =
    new Default[GroupValue.RecordValue](schema) {

      override def start(): Unit =
        this.groupValue = Value.record(Map("key" -> Value.nil, "value" -> Value.nil))

      override def end(): Unit =
        parent.put(name, this.groupValue)

    }

  def root(schema: GroupType): GroupValueConverter[GroupValue.RecordValue] =
    new Default[GroupValue.RecordValue](schema) {

      override def start(): Unit =
        this.groupValue = Value.record(
          this.schema.getFields.asScala.toList.map(_.getName -> Value.nil).toMap
        )

      override def end(): Unit = ()
    }

}
