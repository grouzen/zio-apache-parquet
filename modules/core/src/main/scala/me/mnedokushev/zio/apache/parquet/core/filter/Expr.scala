package me.mnedokushev.zio.apache.parquet.core.filter

import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.filter2.predicate.FilterApi
import me.mnedokushev.zio.apache.parquet.core.Value
import zio.prelude._

sealed trait Expr[-A]

object Expr {

  final case class Column[A](path: String)(implicit val typeTag: TypeTag[A]) extends Expr[A] { self =>

    def /[B: TypeTag](column: Column[B]): Column[B] =
      Column(s"$path.${column.path}")

    def >(value: A)(implicit ev: OperatorSupport.LessGreater[A]): Predicate[A] =
      Predicate.Binary(self, value, Operator.Binary.GreaterThen())

    def ===(value: A)(implicit ev: OperatorSupport.EqNotEq[A]): Predicate[A] =
      Predicate.Binary(self, value, Operator.Binary.Eq())

  }

  sealed trait Predicate[A] extends Expr[A] { self =>

    def not: Predicate[A] =
      Predicate.Unary(self, Operator.Unary.Not[A]())

    def and[B](other: Predicate[B]): Predicate[A] =
      Predicate.Logical(self, other, Operator.Logical.And[A, B])

    def or[B](other: Predicate[B]): Predicate[A] =
      Predicate.Logical(self, other, Operator.Logical.Or[A, B])

  }

  object Predicate {

    final case class Binary[A](column: Column[A], value: A, op: Operator.Binary[A]) extends Predicate[A]

    final case class Unary[A](predicate: Predicate[A], op: Operator.Unary[A]) extends Predicate[A]

    final case class Logical[A, B](left: Predicate[A], right: Predicate[B], op: Operator.Logical[A, B])
        extends Predicate[A]

  }

  def compile[A](predicate: Predicate[A]): Either[String, FilterPredicate] =
    predicate match {
      case Predicate.Unary(predicate0, op)    =>
        op match {
          case Operator.Unary.Not() =>
            compile(predicate0).map(FilterApi.not)
        }
      case Predicate.Logical(left, right, op) =>
        (compile(left) <*> compile(right)).map { case (left0, right0) =>
          op match {
            case Operator.Logical.And() =>
              FilterApi.and(left0, right0)
            case Operator.Logical.Or()  =>
              FilterApi.or(left0, right0)
          }
        }
      case Predicate.Binary(column, value, op) =>
        (column.typeTag, value) match {
          case (TypeTag.String, v: String) =>
            val column0 = FilterApi.binaryColumn(column.path)
            val value0  = Value.string(v).value

            op match {
              case Operator.Binary.Eq()    =>
                Right(FilterApi.eq(column0, value0))
              case Operator.Binary.NotEq() =>
                Right(FilterApi.notEq(column0, value0))
              case _                       => ???
            }
          case (TypeTag.Int, v: Int)       =>
            val column0 = FilterApi.intColumn(column.path)
            val value0  = Int.box(Value.int(v).value)

            op match {
              case Operator.Binary.GreaterThen() =>
                Right(FilterApi.gt(column0, value0))
              case _                             => ???
            }
          case _                           => ???
        }

    }

}
