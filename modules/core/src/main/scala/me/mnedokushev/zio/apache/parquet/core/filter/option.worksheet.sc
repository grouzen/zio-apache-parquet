
// import zio.schema.Schema
// import zio.schema._
// import zio.schema.AccessorBuilder
// import zio.schema.DeriveSchema

// type Lens[F, S, A]  = Expr[A]
// type Prism[F, S, A] = Unit
// type Traverse[S, A] = Unit

// case class Expr[A](value: A) {
//   def ===(other: A): Boolean =
//     value == other
// }

// final class ExprBuilder extends AccessorBuilder {

//   override type Lens[F, S, A] = Expr[A]

//   override type Prism[F, S, A] = Unit

//   override type Traversal[S, A] = Unit

//   override def makeLens[F, S, A](product: Schema.Record[S], term: Schema.Field[S, A]): Lens[F, S, A] = {
//     val value = term.schema.defaultValue.toOption.get

//     Expr(value)
//   }

//   override def makePrism[F, S, A](sum: Schema.Enum[S], term: Schema.Case[S, A]): Prism[F, S, A] = ()

//   override def makeTraversal[S, A](collection: Schema.Collection[S, A], element: Schema[A]): Traversal[S, A] = ()

// }

// trait Def[Columns0] {
//   def columns: Columns0
// }

// object Def {

//   def columns[A](implicit schema: Schema[A]): schema.Accessors[Lens, Prism, Traverse] =
//     new Def[schema.Accessors[Lens, Prism, Traverse]] {
//       override val columns: schema.Accessors[Lens, Prism, Traverse] =
//         schema.makeAccessors(new ExprBuilder)
//     }.columns

// }

// case class Record(a: Option[Int])
// object Record {

//   def optionless[A](schema0: Schema[_]): Schema[A] =
//     schema0 match {
//       case c @ Schema.CaseClass1(_, _, _, _) =>
//         caseClass1(c)
//       case s: Schema.Optional[_]             =>
//         val v = optionless[A](s.schema.asInstanceOf[Schema[A]])
//         v
//       case s                                 =>
//         s.asInstanceOf[Schema[A]]
//     }

//   def caseClass1[A, Z](c: Schema.CaseClass1[A, Z]): Schema.CaseClass1[Any, Any] =
//     Schema.CaseClass1(
//       c.id,
//       Schema.Field(
//         c.field.name,
//         optionless(c.field.schema),
//         c.field.annotations,
//         c.field.validation,
//         c.field.get,
//         c.field.set
//       ),
//       c.defaultConstruct,
//       c.annotations
//     )

//   val schema  = DeriveSchema.gen[Record]
//   val schema0 = optionless(schema)

// }
