package ru.naissur.ch03;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;

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

    Dataset<Row> df2 = bookDs.toDF();
    df2 = df2.withColumn(
        "releaseDateAsString",
        concat(
            expr("releaseDate.year + 1900"), lit("-"),
            expr("releaseDate.month + 1"), lit("-"),
            df2.col("releaseDate.date")));

    df2 = df2.withColumn(
            "releaseDateAsDate",
            to_date(df2.col("releaseDateAsString"), "yyyy-M-d"))
        .drop("releaseDateAsString");
    System.out.println("*** Books are back in a dataframes");
    df2.show(5);
    df2.printSchema();
  }

}
