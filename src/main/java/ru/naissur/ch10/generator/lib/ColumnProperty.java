package ru.naissur.ch10.generator.lib;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * Defines the properties of a column for a record.
 *
 * @author jgp
 */
@Setter
@Getter
@AllArgsConstructor
public class ColumnProperty {

  private FieldType recordType;
  private String option;

}
