package me.mnedokushev.zio.apache.parquet.core.filter

sealed trait OperatorSupport[A] {
  def typeTag: TypeTag[A]
}

object OperatorSupport {

  abstract class LessGreater[A: TypeTag] extends OperatorSupport[A] {
    override def typeTag: TypeTag[A] = implicitly[TypeTag[A]]
  }

  object LessGreater {
    implicit case object Byte  extends LessGreater[Byte]
    implicit case object Short extends LessGreater[Short]
    implicit case object Int   extends LessGreater[Int]
  }

  abstract class EqNotEq[A: TypeTag] extends OperatorSupport[A] {
    override def typeTag: TypeTag[A] = implicitly[TypeTag[A]]
  }

  object EqNotEq {
    implicit case object String  extends EqNotEq[String]
    implicit case object Boolean extends EqNotEq[Boolean]
    implicit case object Byte    extends EqNotEq[Byte]
    implicit case object Short   extends EqNotEq[Short]
  }

}
