package com.bbva.datiocoursework

import org.apache.spark.sql.{DataFrame, SparkSession}


class Joins(spark: SparkSession) {

  // Add implicit encoders
  import spark.implicits._

  val left: DataFrame = Seq(
    (1, "A"), (2, "A"), (1, "B"), (2, "B"), (3, "B")
  ).toDF("F1", "F2")

  val right: DataFrame = Seq(
    ("A", 1), ("A", 2), ("A", 3), ("C", 1), ("C", 2)
  ).toDF("F2", "F3")

  def run(): Unit = {

    println("left")
    left.show

    println("right")
    right.show

    println("CROSS")
    val cross = left.crossJoin(right)
    cross.show(100)

    println("INNER")
    left.join(right, Seq("F2"), "inner").show

    println("FULL")
    left.join(right, Seq("F2"), "full").show

    println("LEFT_ANTI")
    left.join(right, Seq("F2"), "left_anti").show

    println("LEFT_SEMI")
    left.join(right, Seq("F2"), "left_semi").show

    println("LEFT")
    left.join(right, Seq("F2"), "left").show

    println("RIGHT")
    left.join(right, Seq("F2"), "right").show

  }

}
