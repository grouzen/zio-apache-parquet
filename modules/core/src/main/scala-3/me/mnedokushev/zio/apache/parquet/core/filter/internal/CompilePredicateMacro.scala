package me.mnedokushev.zio.apache.parquet.core.filter.internal

import me.mnedokushev.zio.apache.parquet.core.filter.Predicate
import org.apache.parquet.filter2.predicate.FilterPredicate

import scala.quoted.*

object CompilePredicateMacro {

  private class PredicateFromExpr[A] extends FromExpr[Predicate[A]] {

    override def unapply(x: Expr[Predicate[A]])(using Quotes): Option[Predicate[A]] = {
      import quotes.reflect.*

      println(x.asTerm.show(using Printer.TreeStructure))

      x.asTerm.tpe.widenTermRefByName match {
        case ConstantType(c) => Some(c.value.asInstanceOf[Predicate[A]])
        case _               => None
      }
    }

  }

  // TODO: tests
  def compileImpl[A: Type](predicate: Expr[Predicate[A]])(using Quotes): Expr[FilterPredicate] = {
    import quotes.reflect.*

    // Example of a type representation of A type:
    // AndType(
    //   AndType(
    //     TypeRef(TermRef(ThisType(TypeRef(NoPrefix(), "scala")), "Predef"), "String"),
    //     AppliedType(
    //       TypeRef(TermRef(ThisType(TypeRef(NoPrefix(), "<root>")), "scala"), "Option"),
    //       List(
    //         TypeRef(TermRef(ThisType(TypeRef(NoPrefix(), "<root>")), "scala"), "Int")
    //       )
    //     )
    //   ),
    //   TypeRef(TermRef(ThisType(TypeRef(NoPrefix(), "<root>")), "scala"), "Int")
    // )
    // TODO: rewrite using limited stack for safety
    def containsOptionalValue(tpe: TypeRepr): Boolean =
      tpe match {
        case AndType(a, b)       =>
          containsOptionalValue(a) || containsOptionalValue(b)
        case AppliedType(tpe, _) =>
          containsOptionalValue(tpe)
        case TypeRef(_, name)    =>
          List("Option", "Some", "None").contains(name)
      }

    if (containsOptionalValue(TypeRepr.of[A]))
      report.errorAndAbort(
        s"""
           | The use of optional columns in filter predicate is prohibited. Please, use .nullable:
           |   column.nullable > 3
           | Predicate tree: ${predicate.show}
        """.stripMargin
      )
    else {
      val pred0 = predicate.valueOrAbort(using PredicateFromExpr[A])

      _root_.me.mnedokushev.zio.apache.parquet.core.filter.Filter.compile0(pred0) match {
        case Right(filter) =>
          val clazz = filter.getClass()
          Ref(defn.Predef_classOf)
            .appliedToType(TypeRepr.typeConstructorOf(clazz))
            .asExpr
            .asInstanceOf[Expr[FilterPredicate]]
        case Left(error)   =>
          report.errorAndAbort(s"Error while compiling filter predicate: $error")
      }
    }

  }

}
