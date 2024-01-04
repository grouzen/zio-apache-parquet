package me.mnedokushev.zio.apache.parquet.core.filter

import zio.schema.Schema
import zio.schema.StandardType

sealed trait TypeTag[+A]

object TypeTag {

  implicit case object String  extends TypeTag[String]
  implicit case object Boolean extends TypeTag[Boolean]
  implicit case object Byte    extends TypeTag[Byte]
  implicit case object Short   extends TypeTag[Short]
  implicit case object Int     extends TypeTag[Int]
  implicit case object Long    extends TypeTag[Long]

  def deriveTypeTag[A](schema: Schema[A]): Option[TypeTag[A]] =
    schema match {
      case s: Schema.Lazy[_]                 => deriveTypeTag(s.schema)
      case s: Schema.Optional[_]             => deriveTypeTag(s.schema.asInstanceOf[Schema[A]])
      case Schema.Primitive(standartType, _) => deriveTypeTag(standartType)
      case _                                 => None
    }

  def deriveTypeTag[A](standartType: StandardType[A]): Option[TypeTag[A]] =
    standartType match {
      case StandardType.StringType => Some(TypeTag.String)
      case StandardType.BoolType   => Some(TypeTag.Boolean)
      case StandardType.ByteType   => Some(TypeTag.Byte)
      case StandardType.ShortType  => Some(TypeTag.Short)
      case StandardType.IntType    => Some(TypeTag.Int)
      case StandardType.LongType   => Some(TypeTag.Long)
      case _                       => None
    }

}
