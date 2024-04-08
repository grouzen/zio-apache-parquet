package me.mnedokushev.zio.apache.parquet.core.filter

trait Column[A] { self =>

  type Identity

  val path: String
  val typeTag: TypeTag[A]

  // TODO: overcome the limitation of scala macros for having a better API
  // I found out the compiler throws an error that macro is not found as 
  // the macro itself depends on Column. The only option is to move the definition 
  // of "concat" outside the Column class.
  // def /[B](child: Column[B]): Column[B] = 
  //   ColumnPathConcatMacro.concatImpl[A, B]

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

  final case class Named[A: TypeTag, Identity0](path: String) extends Column[A] {
    override type Identity = Identity0
    override val typeTag: TypeTag[A] = implicitly[TypeTag[A]]
  }

}
