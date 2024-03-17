package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.filter.internal.ColumnPathConcatMacro


trait Column[A] { self =>

  type Identity

  val path: String
  val typeTag: TypeTag[A]

  // TODO: validate parent/child relation via macros
  def /[B: TypeTag](child: Column[B]): Column[B] =
    macro ColumnPathConcatMacro.concatImpl[B]
    // Column[B](path = s"$path.${child.path}")

  def >(value: A)(implicit ev: OperatorSupport.LtGt[A]): Predicate[A] =
    Predicate.Binary(self, value, Operator.Binary.GreaterThen())

  def <(value: A)(implicit ev: OperatorSupport.LtGt[A]): Predicate[A] =
    Predicate.Binary(self, value, Operator.Binary.LessThen())

  def >=(value: A)(implicit ev: OperatorSupport.LtGt[A]): Predicate[A] =
    Predicate.Binary(self, value, Operator.Binary.GreaterEq())

  def <=(value: A)(implicit ev: OperatorSupport.LtGt[A]): Predicate[A] =
    Predicate.Binary(self, value, Operator.Binary.LessEq())

  def ===(value: A)(implicit ev: OperatorSupport.EqNotEq[A]): Predicate[A] =
    Predicate.Binary(self, value, Operator.Binary.Eq())

  def =!=(value: A)(implicit ev: OperatorSupport.EqNotEq[A]): Predicate[A] =
    Predicate.Binary(self, value, Operator.Binary.NotEq())

  def in(values: Set[A])(implicit ev: OperatorSupport.EqNotEq[A]): Predicate[A] =
    Predicate.BinarySet(self, values, Operator.Binary.Set.In())

  def notIn(values: Set[A])(implicit ev: OperatorSupport.EqNotEq[A]): Predicate[A] =
    Predicate.BinarySet(self, values, Operator.Binary.Set.NotIn())

}

object Column {

  type Aux[A, Identity0] = Column[A] { 
    type Identity = Identity0
  }

  final case class Named[A: TypeTag, Identity0](path: String) extends Column[A] {
    override type Identity = Identity0
    override val typeTag: TypeTag[A] = implicitly[TypeTag[A]]
  }

}
