package org.apache.parquet.parqour.query.tokens;


import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexPosition;

/**
 * Created by sircodesalot on 15/4/2.
 */
public abstract class ParquelToken {
  private final ParquelExpressionType type;
  private ParquelLexPosition position;

  public ParquelToken(ParquelLexPosition position, ParquelExpressionType type) {
    this.position = position;
    this.type = type;
  }

  public ParquelLexPosition position() {
    return this.position;
  }

  public ParquelExpressionType type() {
    return this.type;
  }

  public boolean is(ParquelExpressionType type) {
    return this.type == type;
  }

  public boolean is(ParquelExpressionType type, String representation) {
    return this.type == type && representation.equalsIgnoreCase(this.toString());
  }

  public boolean isMatchingCase(ParquelExpressionType type, String representation) {
    return this.type == type && representation.equals(this.toString());
  }
}
