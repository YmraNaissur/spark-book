package ru.naissur.ch03;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IngestionSchemaManipulationApp {

  public static void main(String[] args) {
    IngestionSchemaManipulationApp app = new IngestionSchemaManipulationApp();
    app.start();
  }

  private void start() {
    // Создание сеанса Spark
    SparkSession spark = SparkSession.builder()
        .appName("Restaurants in Wake County, NC")
        .master("local")
        .getOrCreate();

    // Создание фрейма данных
    Dataset<Row> df = spark.read()
        .format("csv")
        .option("header", true)
        .load("data/Restaurants_in_Wake_County_NC.csv");

    System.out.println("*** Right after ingestion");
    df.show(5);

    df.printSchema();
    System.out.printf("We have %d records.%n", df.count());

    df = df.withColumn("county", lit("Wake")) // Создаётся новый столбец с именем county, содержащий значение Wake в каждой записи.
        // Переименовываются столбцы для соответствия именам столбцов в новом наборе данных.
        .withColumnRenamed("HSISID", "datasetId")
        .withColumnRenamed("NAME", "name")
        .withColumnRenamed("ADDRESS1", "address1")
        .withColumnRenamed("ADDRESS2", "address2")
        .withColumnRenamed("CITY", "city")
        .withColumnRenamed("STATE", "state")
        .withColumnRenamed("POSTALCODE", "zip")
        .withColumnRenamed("PHONENUMBER", "tel")
        .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
        .withColumnRenamed("FACILITYTYPE", "type")
        .withColumnRenamed("X", "geoX")
        .withColumnRenamed("Y", "geoY")
        // Удаление указанных столбцов
        .drop("OBJECTID")
        .drop("PERMITID")
        .drop("GEOCODESTATUS");

    df = df.withColumn("id", concat(
        df.col("state"), lit("_"),
        df.col("county"), lit("_"),
        df.col("datasetId")));

    System.out.println("*** Dataframe transformed");
    df.show(5);
    df.printSchema();
  }

}
