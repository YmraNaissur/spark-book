package ru.naissur;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class CsvToDatabaseApp {

  public static void main(String[] args) {
    CsvToDatabaseApp app = new CsvToDatabaseApp();
    app.start();
  }

  private void start() {
    // создание сеанса на локальном ведущем узле
    SparkSession spark = SparkSession.builder()
        .appName("CSV to Database")
        .master("local")
        .getOrCreate();

    // Считывание CSV-файла authors.csv с заголовком и сохранение его содержимого в базе данных
    Dataset<Row> df = spark.read()
        .format("csv")
        .option("header", true)
        .load("data/authors.csv");

    // Создание нового столбца с именем name как объединения name, запятой и fname
    df = df.withColumn(
        "name",
        concat(df.col("lname"), lit(", "), df.col("fname")));

    // URL для установления соединения
    String dbConnectionUrl = "jdbc:postgresql://localhost/postgres";

    // Свойства для установления соединения с БД
    // Драйвер JDBC указан в pom.xml
    Properties prop = new Properties();
    prop.setProperty("driver", "org.postgresql.Driver");
    prop.setProperty("user", "postgres");
    prop.setProperty("password", "admin");

    // Перезапись содержимого таблицы с именем ch02
    df.write()
        .mode(SaveMode.Overwrite)
        .jdbc(dbConnectionUrl, "ch02", prop);

    System.out.println("Process complete");
  }

}
