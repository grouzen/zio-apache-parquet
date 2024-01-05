package me.mnedokushev.zio.apache.parquet.core.filter

import scala.annotation.implicitNotFound

sealed trait OperatorSupport[A]

object OperatorSupport {

  @implicitNotFound("You can't use this operator for the type ${A}")
  abstract class LtGt[A: TypeTag] extends OperatorSupport[A]

  object LtGt {
    implicit case object SByte  extends LtGt[Byte]
    implicit case object SShort extends LtGt[Short]
    implicit case object SInt   extends LtGt[Int]
  }

  @implicitNotFound("You can't use this operator for the type ${A}")
  abstract class EqNotEq[A: TypeTag] extends OperatorSupport[A]

  object EqNotEq {
    implicit case object SString  extends EqNotEq[String]
    implicit case object SBoolean extends EqNotEq[Boolean]
    implicit case object SByte    extends EqNotEq[Byte]
    implicit case object SShort   extends EqNotEq[Short]
    implicit case object SInt     extends EqNotEq[Int]
  }

}
