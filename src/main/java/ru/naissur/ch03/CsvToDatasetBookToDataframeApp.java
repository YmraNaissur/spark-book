package ru.naissur.ch03;

import java.io.Serializable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.naissur.ch03.mapper.BookMapper;
import ru.naissur.ch03.model.Book;

public class CsvToDatasetBookToDataframeApp implements Serializable {

  public static void main(String[] args) {
    var app = new CsvToDatasetBookToDataframeApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("CSV to dataframe to Dataset<Book> and back")
        .master("local")
        .getOrCreate();

    String fileName = "data/books.csv";
    Dataset<Row> df = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(fileName);

    System.out.println("*** Books ingested in a dataframe");
    df.show(5);
    df.printSchema();

    Dataset<Book> bookDs = df.map(
        new BookMapper(),
        Encoders.bean(Book.class));

    System.out.println("*** Books are now in a dataset of books");
    bookDs.show(5);
    bookDs.printSchema();
  }

}
