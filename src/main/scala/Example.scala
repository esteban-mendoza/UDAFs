package com.bbva.datiocoursework

import utils._

import org.apache.log4j._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Example {
  def main(args: Array[String]): Unit = {

    // Set log level to WARN
    Logger.getLogger("org").setLevel(Level.WARN)

    // Create SparkSession
    val spark = SparkSession
      .builder
      .appName("CustomAverage")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // Create schema
    val pokemonSchema = new StructType()
      .add("index", IntegerType)
      .add("name", StringType)
      .add("type1", StringType)
      .add("type2", StringType)
      .add("total", IntegerType)
      .add("hp", IntegerType)
      .add("attack", IntegerType)
      .add("defense", IntegerType)
      .add("spAttack", IntegerType)
      .add("spDefense", IntegerType)
      .add("speed", IntegerType)
      .add("generation", IntegerType)
      .add("legendary", BooleanType)

    // Import implicit encoders
    import spark.implicits._

    // Load data
    val dataset = spark
      .read
      .schema(pokemonSchema)
      .option("header", "true")
      .csv("src/test/resources/data/csv/Pokemon.csv")
      .as[Pokemon]

    // Declare UDAFs
    val customAvg = udaf(CustomAverage)
    val customCorr = udaf(Correlation)

    // Use UDAF over RelationalGroupedDataset
    val x = dataset
      .groupBy(col("generation"))
      .agg(
        customAvg(col("attack")).alias("avgAttack"),
        customAvg(col("defense")).alias("avgDefense")
      )

    // Use UDAF with WindowSpec
    val window: WindowSpec = Window.partitionBy(dataset("generation"))

    val avgAttack: Column = customAvg(dataset("attack"))
      .over(window)
      .alias("avgAttack")
    val avgDefense: Column = customAvg(dataset("defense"))
      .over(window)
      .alias("avgDefense")

    val y = dataset.select(
      dataset.columns.map(col) :+ avgAttack :+ avgDefense: _*
    )

    // Calculate correlation between attack and defense
    val rAttackDefense: Column = customCorr(y("attack"), y("avgAttack"), y("defense"), y("avgDefense"))
      .over(window)
      .alias("rAttackDefense")

    val z = y.groupBy(col("generation"))
      .agg(
        customCorr(y("attack"), y("avgAttack"), y("defense"), y("avgDefense")).alias("rAttackDefense")
      )

    val w = y.select(
      y.columns.map(col) :+ rAttackDefense: _*
    )

    // Show results
    x.printSchema
    x.show

    y.printSchema
    y.show

    z.printSchema
    z.show

    w.printSchema
    w.show

    // Stop spark session
    spark.stop()
  }
}
