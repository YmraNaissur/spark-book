package ru.naissur.ch05;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PiComputeApp implements Serializable {

  private static long counter = 0;

  public static void main(String[] args) {
    var app = new PiComputeApp();
    app.start(10);
  }

  private void start(int slices) {
    int numberOfThrows = 100_000 * slices;
    System.out.println("About to throw " + numberOfThrows + " darts, ready? Stay away from the target!");

    // Initialize session
    long t0 = System.currentTimeMillis();
    SparkSession spark = SparkSession.builder()
        .appName("Spark PI compute app")
        .master("local[*]")
        .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    long t1 = System.currentTimeMillis();
    System.out.println("Session initialized in " + (t1 - t0) + " ms");

    // Create initial dataframe
    List<Integer> listOfThrows = new ArrayList<>(numberOfThrows);
    for (int i = 0; i < numberOfThrows; i++) {
      listOfThrows.add(i);
    }
    Dataset<Row> incrementalDf = spark
        .createDataset(listOfThrows, Encoders.INT())
        .toDF();
    long t2 = System.currentTimeMillis();
    System.out.println("Initial dataframe built in " + (t2 - t1) + " ms");

    // map throws
    Dataset<Integer> dartsDs = incrementalDf
        .map(new DartMapper(), Encoders.INT());
    long t3 = System.currentTimeMillis();
    System.out.println("Throwing darts done in " + (t3 - t2) + " ms");

    // reducing result
    int dartsInCircle = dartsDs.reduce(new DartReducer());
    long t4 = System.currentTimeMillis();
    System.out.println("Analyzing result in " + (t4 - t3) + " ms");
    System.out.println("PI is roughly " + 4.0 * dartsInCircle / numberOfThrows);
  }

  static class DartMapper implements MapFunction<Row, Integer> {
    @Override
    public Integer call(Row r) throws Exception {
      double x = Math.random() * 2 - 1;
      double y = Math.random() * 2 - 1;
      counter++;
      if (counter % 100_000 == 0) {
        System.out.println(counter + " darts thrown so far");
      }
      return (x * x + y * y <= 1) ? 1 : 0;
    }
  }

  static class DartReducer implements ReduceFunction<Integer> {
    @Override
    public Integer call(Integer x, Integer y) throws Exception {
      return x + y;
    }
  }
}



