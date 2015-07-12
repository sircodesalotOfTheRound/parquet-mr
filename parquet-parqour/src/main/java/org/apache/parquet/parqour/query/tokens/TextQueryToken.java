package org.apache.parquet.parqour.query.tokens;


import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexPosition;

/**
 * Created by sircodesalot on 15/4/2.
 */
public abstract class TextQueryToken {
  private final TextQueryExpressionType type;
  private ParquelLexPosition position;

  public TextQueryToken(ParquelLexPosition position, TextQueryExpressionType type) {
    this.position = position;
    this.type = type;
  }

  public ParquelLexPosition position() {
    return this.position;
  }

  public TextQueryExpressionType type() {
    return this.type;
  }

  public boolean is(TextQueryExpressionType type) {
    return this.type == type;
  }

  public boolean is(TextQueryExpressionType type, String representation) {
    return this.type == type && representation.equalsIgnoreCase(this.toString());
  }

  public boolean isMatchingCase(TextQueryExpressionType type, String representation) {
    return this.type == type && representation.equals(this.toString());
  }
}
