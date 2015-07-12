package org.apache.parquet.parqour.query.tokens;


import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelCharacterStream;

/**
 * In theory, this class should never be used.
 */
public class TextQueryMysteryToken extends TextQueryToken {
  private final char character;

  private TextQueryMysteryToken(ParquelCharacterStream stream) {
    super(stream.position(), TextQueryExpressionType.UNKNOWN);
    this.character = stream.readCurrentAndAdvance();
  }

  public static TextQueryMysteryToken read(ParquelCharacterStream stream) {
    return new TextQueryMysteryToken(stream);
  }

  @Override
  public String toString() {
    return ((Character)character).toString();
  }

  public char character() { return this.character; }
}