package ru.naissur.ch10.consumer;

import java.util.concurrent.TimeoutException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import ru.naissur.ch10.generator.lib.StreamingUtils;

public class ReadLinesFromFileStreamApp {

  public static void main(String[] args) {
    ReadLinesFromFileStreamApp app = new ReadLinesFromFileStreamApp();
    try {
      app.start();
    } catch (TimeoutException e) {
      System.out.println("A timeout exception has occured: " + e.getMessage());
    }
  }

  private void start() throws TimeoutException {
    System.out.println("-> start()");

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
    Logger.getLogger("org.sparkproject").setLevel(Level.WARN);
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN);
    Logger.getLogger("io.netty").setLevel(Level.WARN);

    SparkSession spark = SparkSession.builder()
        .appName("Read lines over a file stream")
        .master("local")
        .getOrCreate();

    Dataset<Row> df = spark
        .readStream()
        .format("text")
        .load(StreamingUtils.getInputDirectory());

    StreamingQuery query = df
        .writeStream()
        .outputMode(OutputMode.Append())
        .format("console")
        .option("truncate", false)
        .option("numRows", 3)
        .start();

    try {
      query.awaitTermination(60000);
    } catch (StreamingQueryException e) {
      System.out.println("Exception while waiting for query to end " + e.getMessage());
    }

    System.out.println("<- start()");
  }

}
