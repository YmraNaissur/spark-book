package ru.naissur.ch04;

import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TransformationExplainApp {

  public static void main(String[] args) {
    var app = new TransformationExplainApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Transformation explain App")
        .master("local")
        .getOrCreate();

    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .load("data/ch04/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv");

    Dataset<Row> initialDf = df;

    df = df.union(initialDf);

    df = df.withColumn("lcl", df.col("Lower Confidence Limit").cast("double"));
    df = df.withColumn("ucl", df.col("Upper Confidence Limit").cast("double"));
    df = df
        .withColumn("avg", expr("(lcl+ucl)/2"))
        .withColumn("lcl2", df.col("lcl"))
        .withColumn("ucl2", df.col("ucl"));

    df.explain();

    spark.stop();
  }
}
