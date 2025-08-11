package ru.naissur.ch04;

import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TransformationAndActionApp {

  public static void main(String[] args) {
    var app = new TransformationAndActionApp();
    String mode = "noop";
    if (args.length != 0) {
      mode = args[0];
    }
    app.start(mode);
  }

  private void start(String mode) {
    // Создание сеанса
    long t0 = System.currentTimeMillis();
    SparkSession spark = SparkSession.builder()
        .appName("Analysing Catalyst's behavior")
        .master("local")
        .getOrCreate();
    long t1 = System.currentTimeMillis();
    System.out.println("1. Creating a session ........... " + (t1 - t0));

    // Считывание файла
    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .load("data/ch04/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv");
    Dataset<Row> initialDf = df;
    long t2 = System.currentTimeMillis();
    System.out.println("2. Loading initial dataset ....... " + (t2 - t1));

    // Увеличение набора данных
    for (int i = 0; i < 60; i++) {
      df = df.union(initialDf);
    }
    long t3 = System.currentTimeMillis();
    System.out.println("3. Building full dataset ........ " + (t3 - t2));

    // Очистка данных (переименование столбцов и приведение типа полей lcl и ucl к double)
    df = (df
        .withColumn("lcl", df.col("Lower Confidence Limit").cast("double"))
        .withColumn("ucl", df.col("Upper Confidence Limit").cast("double"))
        .drop("Lower Confidence Limit", "Upper Confidence Limit")
    );
    long t4 = System.currentTimeMillis();
    System.out.println("4. Clean-up .................. " + (t4 - t3));

    // Преобразование данных в различных режимах
    if (mode.compareToIgnoreCase("noop") != 0) {
      df = df
          .withColumn("avg", expr("(lcl+ucl)/2"))
          .withColumn("lcl2", df.col("lcl"))
          .withColumn("ucl2", df.col("ucl"));
      if (mode.compareToIgnoreCase("full") == 0) {
        df = df
            .drop("avg")
            .drop("lcl2")
            .drop("ucl2");
      }
    }
    long t5 = System.currentTimeMillis();
    System.out.println("5. Transformations .......... " + (t5 - t4));

    // Вызов действия
    df.collect();
    long t6 = System.currentTimeMillis();
    System.out.println("6. Final action ............ " + (t6 - t5));

    System.out.println();
    System.out.println("# of records .............. " + df.count());
  }

}
