package me.mnedokushev.zio.apache.parquet.core.filter

import org.apache.parquet.filter2.predicate.{ FilterApi, FilterPredicate, Operators }
import zio.prelude._
// import zio.Tag

sealed trait Expr[+A]

object Expr {

  final case class Column[A](path: String, typeTag: TypeTag[A]) extends Expr[A] { self =>

    def >(value: A)(implicit ev: OperatorSupport.LtGt[A]): Predicate[A] =
      Predicate.Binary(self, value, Operator.Binary.GreaterThen())

    def ===(value: A)(implicit ev: OperatorSupport.EqNotEq[A]): Predicate[A] =
      Predicate.Binary(self, value, Operator.Binary.Eq())

  }

  sealed trait Predicate[A] extends Expr[A] { self =>

    def and[B](other: Predicate[B]): Predicate[A] =
      Predicate.Logical(self, other, Operator.Logical.And[A, B]())

    def or[B](other: Predicate[B]): Predicate[A] =
      Predicate.Logical(self, other, Operator.Logical.Or[A, B]())

  }

  def not[A](pred: Predicate[A]): Predicate[A] =
    Predicate.Unary(pred, Operator.Unary.Not[A]())

  object Predicate {

    final case class Binary[A](column: Column[A], value: A, op: Operator.Binary[A]) extends Predicate[A]

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
        (column.typeTag, column.typeTag, value) match {
          case (tt: TypeTag.EqNotEq[_], TypeTag.TString, v: String)   =>
            val tt0 = tt.cast[A]
            handleEqNotEq(tt0.column(column.path), tt0.value(v), op)
          case (tt: TypeTag.EqNotEq[_], TypeTag.TBoolean, v: Boolean) =>
            val tt0 = tt.cast[A]
            handleEqNotEq(tt0.column(column.path), tt0.value(v), op)
          case (tt: TypeTag.LtGt[_], TypeTag.TByte, v: Byte)          =>
            val tt0 = tt.cast[A]
            handleLtGt(tt0.column(column.path), tt0.value(v), op)
          case (tt: TypeTag.LtGt[_], TypeTag.TInt, v: Int)            =>
            val tt0 = tt.cast[A]
            handleLtGt(tt0.column(column.path), tt0.value(v), op)
          case _                                                      => ???
        }

    }
  }

}
