package me.mnedokushev.zio.apache.parquet.core.filter

import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate, Operators}
import zio.prelude._

sealed trait Expr[+A]

object Expr {

  final case class Column[A](path: String, typeTag: TypeTag[A]) extends Expr[A] { self =>

    def >(value: A)(implicit ev: OperatorSupport.LtGt[A]): Predicate[A] =
      Predicate.Binary(self, value, Operator.Binary.GreaterThen())

    def ===(value: A)(implicit ev: OperatorSupport.EqNotEq[A]): Predicate[A] =
      Predicate.Binary(self, value, Operator.Binary.Eq())

    def in(values: Set[A])(implicit ev: OperatorSupport.EqNotEq[A]): Predicate[A] =
      Predicate.BinarySet(self, values, Operator.Binary.Set.In())

    def notIn(values: Set[A])(implicit ev: OperatorSupport.EqNotEq[A]): Predicate[A] =
      Predicate.BinarySet(self, values, Operator.Binary.Set.NotIn())

  }

  sealed trait Predicate[A] extends Expr[A] { self =>

    def &&[B](other: Predicate[B]): Predicate[A] =
      Predicate.Logical(self, other, Operator.Logical.And[A, B]())

    def ||[B](other: Predicate[B]): Predicate[A] =
      Predicate.Logical(self, other, Operator.Logical.Or[A, B]())

  }

  object Predicate {

    final case class Binary[A](column: Column[A], value: A, op: Operator.Binary[A]) extends Predicate[A]

    final case class BinarySet[A](column: Column[A], values: Set[A], op: Operator.Binary.Set[A]) extends Predicate[A]

    final case class Unary[A](predicate: Predicate[A], op: Operator.Unary[A]) extends Predicate[A]

    final case class Logical[A, B](left: Predicate[A], right: Predicate[B], op: Operator.Logical[A, B])
        extends Predicate[A]

  }

  def compile[A](predicate: Predicate[A]): Either[String, FilterPredicate] = {

    def binarySet[T <: Comparable[T], C <: Operators.Column[T] with Operators.SupportsEqNotEq](
      column: C,
      values: java.util.Set[T],
      op: Operator.Binary.Set[_]
    ) =
      op match {
        case Operator.Binary.Set.In()    =>
          Right(FilterApi.in(column, values))
        case Operator.Binary.Set.NotIn() =>
          Right(FilterApi.notIn(column, values))
      }

    predicate match {
      case Predicate.Unary(predicate0, op)         =>
        op match {
          case Operator.Unary.Not() =>
            compile(predicate0).map(FilterApi.not)
        }
      case Predicate.Logical(left, right, op)      =>
        (compile(left) <*> compile(right)).map { case (left0, right0) =>
          op match {
            case Operator.Logical.And() =>
              FilterApi.and(left0, right0)
            case Operator.Logical.Or()  =>
              FilterApi.or(left0, right0)
          }
        }
      case Predicate.Binary(column, value, op)     =>
        column.typeTag match {
          case typeTag: TypeTag.EqNotEq[_] =>
            val typeTag0 = typeTag.cast[A]
            val column0  = typeTag0.column(column.path)
            val value0   = typeTag0.value(value)

            op match {
              case Operator.Binary.Eq()    =>
                Right(FilterApi.eq(column0, value0))
              case Operator.Binary.NotEq() =>
                Right(FilterApi.notEq(column0, value0))
              case _                       =>
                Left("missing eqnoteq")
            }
          case typeTag: TypeTag.LtGt[_]    =>
            val typeTag0 = typeTag.cast[A]
            val column0  = typeTag0.column(column.path)
            val value0   = typeTag0.value(value)

            op match {
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
              case _                             =>
                Left("missing ltgt")
            }
          case _                           =>
            Left("missing binaryset")
        }
      case Predicate.BinarySet(column, values, op) =>
        column.typeTag match {
          case typeTag: TypeTag.EqNotEq[_] =>
            val typeTag0 = typeTag.cast[A]
            val column0  = typeTag0.column(column.path)
            val values0  = typeTag0.values(values)

            binarySet(column0, values0, op)
          case typeTag: TypeTag.LtGt[_]    =>
            val typeTag0 = typeTag.cast[A]
            val column0  = typeTag0.column(column.path)
            val values0  = typeTag0.values(values)

            binarySet(column0, values0, op)
          case _                           =>
            Left("missing binaryset")
        }
    }

  }

}
