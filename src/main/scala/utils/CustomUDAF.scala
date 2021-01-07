package com.bbva.datiocoursework
package utils

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator

object CustomUDAF extends Aggregator[Int, Int, Int] {

  override def zero: Int = ???

  override def reduce(b: Int, a: Int): Int = ???

  override def merge(b1: Int, b2: Int): Int = ???

  override def finish(reduction: Int): Int = ???

  override def bufferEncoder: Encoder[Int] = ???

  override def outputEncoder: Encoder[Int] = ???
}
