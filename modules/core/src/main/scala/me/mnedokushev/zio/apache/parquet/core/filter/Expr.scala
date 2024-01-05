package me.mnedokushev.zio.apache.parquet.core.filter

import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.filter2.predicate.FilterApi
// import me.mnedokushev.zio.apache.parquet.core.Value
import zio.prelude._
import org.apache.parquet.filter2.predicate.Operators

sealed trait Expr[-A]

object Expr {

  sealed trait Column[A] extends Expr[A] { self =>
    def typeTag: TypeTag[A]
    def path: String

    def /[B: TypeTag.LtGt](column: ColumnLtGt[B]): ColumnLtGt[B] =
      ColumnLtGt(s"$path.${column.path}")

    def >(value: A)(implicit ev: OperatorSupport.LtGt[A]): Predicate[A] =
      Predicate.Binary(self, value, Operator.Binary.GreaterThen())

    def ===(value: A)(implicit ev: OperatorSupport.EqNotEq[A]): Predicate[A] =
      Predicate.Binary(self, value, Operator.Binary.Eq())
  }

  final case class ColumnDummy[A](path: String)(implicit val typeTag: TypeTag.Dummy[A]) extends Column[A]

  final case class ColumnEqNotEq[A](path: String)(implicit val typeTag: TypeTag.EqNotEq[A]) extends Column[A]

  final case class ColumnLtGt[A](path: String)(implicit val typeTag: TypeTag.LtGt[A]) extends Column[A]

  sealed trait Predicate[A] extends Expr[A] { self =>

    def not: Predicate[A] =
      Predicate.Unary(self, Operator.Unary.Not[A]())

    def and[B](other: Predicate[B]): Predicate[A] =
      Predicate.Logical(self, other, Operator.Logical.And[A, B])

    def or[B](other: Predicate[B]): Predicate[A] =
      Predicate.Logical(self, other, Operator.Logical.Or[A, B])

  }

  object Predicate {

    final case class Binary[A, C <: Column[A]](column: C, value: A, op: Operator.Binary[A]) extends Predicate[A]

    final case class Unary[A](predicate: Predicate[A], op: Operator.Unary[A]) extends Predicate[A]

    final case class Logical[A, B](left: Predicate[A], right: Predicate[B], op: Operator.Logical[A, B])
        extends Predicate[A]

  }

  def compile[A](predicate: Predicate[A]): Either[String, FilterPredicate] = {

    def handleEqNotEq[T <: Comparable[T], C <: Operators.Column[T] with Operators.SupportsEqNotEq](
      column0: C,
      value0: T,
      op: Operator.Binary[_]
    ) = op match {
      case Operator.Binary.Eq()    =>
        Right(FilterApi.eq(column0, value0))
      case Operator.Binary.NotEq() =>
        Right(FilterApi.notEq(column0, value0))
      case _                       =>
        Left("")
    }

    def handleLtGt[T <: Comparable[T], C <: Operators.Column[T] with Operators.SupportsLtGt](
      column0: C,
      value0: T,
      op: Operator.Binary[_]
    ) = op match {
      case Operator.Binary.Eq()          =>
        Right(FilterApi.eq(column0, value0))
      case Operator.Binary.NotEq()       =>
        Right(FilterApi.notEq(column0, value0))
      case Operator.Binary.LessThen()    =>
        Right(FilterApi.lt(column0, value0))
      case Operator.Binary.LessEq()      =>
        Right(FilterApi.ltEq(column0, value0))
      case Operator.Binary.GreaterThen() =>
        Right(FilterApi.gt(column0, value0))
      case Operator.Binary.GreaterEq()   =>
        Right(FilterApi.gtEq(column0, value0))
    }

    predicate match {
      case Predicate.Unary(predicate0, op)     =>
        op match {
          case Operator.Unary.Not() =>
            compile(predicate0).map(FilterApi.not)
        }
      case Predicate.Logical(left, right, op)  =>
        (compile(left) <*> compile(right)).map { case (left0, right0) =>
          op match {
            case Operator.Logical.And() =>
              FilterApi.and(left0, right0)
            case Operator.Logical.Or()  =>
              FilterApi.or(left0, right0)
          }
        }
      case Predicate.Binary(column, value, op) =>
        (column, column.typeTag, value) match {
          case (c: ColumnEqNotEq[A], TypeTag.TString, v: String)   =>
            handleEqNotEq(c.typeTag.column(column.path), c.typeTag.value(v), op)
          case (c: ColumnEqNotEq[A], TypeTag.TBoolean, v: Boolean) =>
            handleEqNotEq(c.typeTag.column(column.path), c.typeTag.value(v), op)
          case (c: ColumnLtGt[A], TypeTag.TByte, v: Byte)          =>
            handleLtGt(c.typeTag.column(column.path), c.typeTag.value(v), op)
          case (c: ColumnLtGt[A], TypeTag.TInt, v: Int)            =>
            handleLtGt(c.typeTag.column(column.path), c.typeTag.value(v), op)
          case _                                                   => ???
        }

    }
  }

}
