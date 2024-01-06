package me.mnedokushev.zio.apache.parquet.core.filter

sealed trait Operator

object Operator {

  sealed trait Binary[A] extends Operator {
    def operatorSupport: OperatorSupport[A]
  }

  object Binary {
    final case class Eq[A: OperatorSupport.EqNotEq]()       extends Binary[A] {
      override def operatorSupport: OperatorSupport[A] = implicitly[OperatorSupport.EqNotEq[A]]
    }
    final case class NotEq[A: OperatorSupport.EqNotEq]()    extends Binary[A] {
      override def operatorSupport: OperatorSupport[A] = implicitly[OperatorSupport.EqNotEq[A]]
    }
    final case class LessThen[A: OperatorSupport.LtGt]()    extends Binary[A] {
      override def operatorSupport: OperatorSupport[A] = implicitly[OperatorSupport.LtGt[A]]
    }
    final case class LessEq[A: OperatorSupport.LtGt]()      extends Binary[A] {
      override def operatorSupport: OperatorSupport[A] = implicitly[OperatorSupport.LtGt[A]]
    }
    final case class GreaterThen[A: OperatorSupport.LtGt]() extends Binary[A] {
      override def operatorSupport: OperatorSupport[A] = implicitly[OperatorSupport.LtGt[A]]
    }
    final case class GreaterEq[A: OperatorSupport.LtGt]()   extends Binary[A] {
      override def operatorSupport: OperatorSupport[A] = implicitly[OperatorSupport.LtGt[A]]
    }

  }

  sealed trait Unary[A] extends Operator

  object Unary {
    final case class Not[A]() extends Unary[A]
  }

  sealed trait Logical[A, B] extends Operator

  object Logical {
    final case class And[A, B]() extends Logical[A, B]
    final case class Or[A, B]()  extends Logical[A, B]
  }

}
