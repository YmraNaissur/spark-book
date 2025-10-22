package ru.naissur.ch10.generator;

import ru.naissur.ch10.generator.lib.FieldType;
import ru.naissur.ch10.generator.lib.RecordGeneratorUtils;
import ru.naissur.ch10.generator.lib.RecordStructure;
import ru.naissur.ch10.generator.lib.RecordWriterUtils;

public class RecordsInFilesGeneratorApp {

  public int streamDuration = 60;
  public int batchSize = 10;
  public int waitTime = 5;

  public static void main(String[] args) {
    RecordStructure rs = new RecordStructure("contact")
        .add("fname", FieldType.FIRST_NAME)
        .add("mname", FieldType.FIRST_NAME)
        .add("lname", FieldType.LAST_NAME)
        .add("age", FieldType.AGE)
        .add("ssn", FieldType.SSN);
    RecordsInFilesGeneratorApp app = new RecordsInFilesGeneratorApp();
    app.start(rs);
  }

  private void start(RecordStructure rs) {
    long start = System.currentTimeMillis();
    while (start + streamDuration * 1000 > System.currentTimeMillis()) {
      int maxRecord = RecordGeneratorUtils.getRandomInt(batchSize) + 1;
      RecordWriterUtils.write(rs.getRecordName() + "_" + System.currentTimeMillis() + ".txt", rs.getRecords(maxRecord, false));
      try {
        Thread.sleep(RecordGeneratorUtils.getRandomInt(waitTime * 1000) + waitTime * 1000 / 2);
      } catch (InterruptedException e) {
        System.out.println("Thread interrupted");
      }
    }
  }

}
