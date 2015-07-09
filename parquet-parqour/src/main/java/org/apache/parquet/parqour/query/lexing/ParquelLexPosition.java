package org.apache.parquet.parqour.query.lexing;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class ParquelLexPosition {
  private final int line;
  private final int column;
  private final int offset;

  public ParquelLexPosition(int line, int column, int offset) {
    this.line = line;
    this.column = column;
    this.offset = offset;
  }

  public int line() {
    return this.line;
  }

  public int column() {
    return this.column;
  }

  public int offset() {
    return this.offset;
  }

  @Override
  public String toString() {
    return String.format("(%s:%s)", line, column);
  }
}
