package me.mnedokushev.zio.apache.parquet.core.filter

final case class Column[A: TypeTag](path: String) { self =>

  val typeTag: TypeTag[A] = implicitly[TypeTag[A]]

  // TODO: validate parent/child relation via macros
  def /[B: TypeTag](child: Column[B]): Column[B] =
    Column[B](path = s"$path.${child.path}")

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
