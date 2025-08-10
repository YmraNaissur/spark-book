package ru.naissur.ch03;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ArrayToDatasetApp {

  public static void main(String[] args) {
    ArrayToDatasetApp app = new ArrayToDatasetApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Array to Dataset App")
        .master("local")
        .getOrCreate();

    String[] strArray = {"Max", "Dasha", "Laguna", "Mysh"};
    List<String> strList = Arrays.asList(strArray);
    Dataset<String> ds = spark.createDataset(strList, Encoders.STRING());

    ds.show();
    ds.printSchema();

    Dataset<Row> df = ds.toDF();
    df.show();
    df.printSchema();
  }

}
