package ru.naissur.ch03.mapper;

import java.io.Serial;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import ru.naissur.ch03.model.Book;

public class BookMapper implements MapFunction<Row, Book> {

  @Serial
  private static final long serialVersionUID = -2L;

  @Override
  public Book call(Row value) throws Exception {
    String dateAsString = value.getAs("releaseDate");
    Date releaseDate = null;
    if (dateAsString != null) {
      SimpleDateFormat parser = new SimpleDateFormat("M/d/yy");
      releaseDate = parser.parse(dateAsString);
    }

    return Book.builder()
        .id(value.getAs("id"))
        .authorId(value.getAs("authorId"))
        .title(value.getAs("title"))
        .releaseDate(releaseDate)
        .link(value.getAs("link"))
        .build();
  }
}
