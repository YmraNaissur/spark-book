package ru.naissur.ch10.generator.lib;

import java.io.Serial;

/**
 * Specific exception to the record generator.
 *
 * @author jgp
 */
public class RecordGeneratorException extends Exception {
  @Serial
  private static final long serialVersionUID = 4046912590125990484L;

  public RecordGeneratorException(String message, Exception e) {
    super(message, e);
  }

}
