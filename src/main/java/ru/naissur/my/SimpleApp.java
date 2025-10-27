package ru.naissur.my;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SimpleApp {

  public static void main(String[] args) {
    var simpleApp = new SimpleApp();
    simpleApp.start();
  }

  private void start() {
    var spark = SparkSession.builder()
        .appName("My Simple Spark Application")
        .master("local[*]")
        .getOrCreate();

    var fileName = "data/my_files/README.md";
    Dataset<String> logData = spark
        .read()
        .textFile(fileName);

    long numAs = logData.filter(col("value").contains("a")).count();
    long numBs = logData.filter(col("value").contains("b")).count();

    System.out.println("Lines with a: " + numAs + "; lines with b: " + numBs);

    logData.show();
  }
}
