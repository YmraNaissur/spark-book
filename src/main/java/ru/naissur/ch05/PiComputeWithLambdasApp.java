package ru.naissur.ch05;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PiComputeWithLambdasApp {

  private static long counter = 0;

  public static void main(String[] args) {
    var app = new PiComputeWithLambdasApp();
    app.start(30);
  }

  private void start(int slices) {
    int numberOrThrows = 100_000 * slices;
    System.out.println("About to throw " + numberOrThrows + " darts, ready? Stay away from the target!");

    long t0 = System.currentTimeMillis();
    SparkSession spark = SparkSession.builder()
        .appName("PI compute with lambdas App")
        .master("local[*]")
        .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    long t1 = System.currentTimeMillis();
    System.out.println("Session initialized in " + (t1 - t0) + " ms");

    List<Integer> listOfThrows = new ArrayList<>(numberOrThrows);
    for (int i = 0; i < numberOrThrows; i++) {
      listOfThrows.add(i);
    }
    Dataset<Row> incrementDf = spark
        .createDataset(listOfThrows, Encoders.INT())
        .toDF();
    long t2 = System.currentTimeMillis();
    System.out.println("Initial dataframe build in " + (t2 - t1) + " ms");

    Dataset<Integer> dartsDs = incrementDf
        .map((MapFunction<Row, Integer>) row -> {
          double x = Math.random() * 2 - 1;
          double y = Math.random() * 2 - 1;
          counter++;
          if (counter % 100_000 == 0) {
            System.out.println(counter + " darts thrown so far");
          }
          return (x * x + y * y <= 1) ? 1 : 0;
        }, Encoders.INT());
    long t3 = System.currentTimeMillis();
    System.out.println("Throwing darts done in " + (t3 - t2) + " ms");

    int dartsInCircle = dartsDs
        .reduce((ReduceFunction<Integer>) Integer::sum);
    long t4 = System.currentTimeMillis();
    System.out.println("Analyzing result in " + (t4 - t3) + " ms");
    System.out.println("PI is roughly " + 4.0 * dartsInCircle / numberOrThrows);

  }

}
