package ru.naissur.ch03.model;

import java.util.Date;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Book {
  int id;
  int authorId;
  String title;
  Date releaseDate;
  String link;
}
