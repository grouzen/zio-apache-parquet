package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.{ Lens, Prism, Traversal }
import org.apache.parquet.filter2.predicate.{ FilterApi, FilterPredicate, Operators }
import zio.prelude._
import zio.schema._
import me.mnedokushev.zio.apache.parquet.core.filter.internal.CompilePredicateMacro

trait Filter {

  type Columns

  val columns: Columns

}

object Filter {

  type Aux[Columns0] = Filter {
    type Columns = Columns0
  }

  def apply[A](implicit
    schema: Schema[A],
    typeTag: TypeTag[A]
  ): Filter.Aux[schema.Accessors[Lens, Prism, Traversal]] =
    new Filter {
      val accessorBuilder =
        new ExprAccessorBuilder(typeTag.asInstanceOf[TypeTag.Record[A]].columns)

      override type Columns =
        schema.Accessors[accessorBuilder.Lens, accessorBuilder.Prism, accessorBuilder.Traversal]

      override val columns: Columns =
        schema.makeAccessors(accessorBuilder)
    }

  inline def compile[A](inline predicate: Predicate[A]): FilterPredicate =
    ${ CompilePredicateMacro.compileImpl[A]('predicate) }

  private[zio] def compile0[A](predicate: Predicate[A]): Either[String, FilterPredicate] = {

    def error(op: Operator) =
      Left(s"Operator $op is not supported by $predicate")

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
            compile0(predicate0).map(FilterApi.not)
        }
      case Predicate.Logical(left, right, op)      =>
        (compile0(left) <*> compile0(right)).map { case (left0, right0) =>
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
                error(op)
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
                error(op)
            }
          case _                           =>
            error(op)
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
            error(op)
        }
    }

  }

  def not[A](pred: Predicate[A]): Predicate[A] =
    Predicate.Unary(pred, Operator.Unary.Not[A]())

}
