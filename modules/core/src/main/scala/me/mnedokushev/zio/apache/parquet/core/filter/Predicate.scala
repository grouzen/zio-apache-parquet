package me.mnedokushev.zio.apache.parquet.core.filter

sealed trait Predicate[A] { self =>

  def and[B](other: Predicate[B]): Predicate[A with B] =
    Predicate.Logical[A, B](self, other, Operator.Logical.And[A, B]())

  def or[B](other: Predicate[B]): Predicate[A with B] =
    Predicate.Logical[A, B](self, other, Operator.Logical.Or[A, B]())

}

object Predicate {

  final case class Binary[A](column: Column[A], value: A, op: Operator.Binary[A]) extends Predicate[A]

  final case class BinarySet[A](column: Column[A], values: Set[A], op: Operator.Binary.Set[A]) extends Predicate[A]

  final case class Unary[A](predicate: Predicate[A], op: Operator.Unary[A]) extends Predicate[A]

  final case class Logical[A, B](left: Predicate[A], right: Predicate[B], op: Operator.Logical[A, B])
      extends Predicate[A with B]

}
