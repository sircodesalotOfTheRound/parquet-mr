package org.apache.parquet.parqour.query.tokens;


import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelCharacterStream;

/**
 * In theory, this class should never be used.
 */
public class ParquelMysteryToken extends ParquelToken {
  private final char character;

  private ParquelMysteryToken(ParquelCharacterStream stream) {
    super(stream.position(), ParquelExpressionType.UNKNOWN);
    this.character = stream.readCurrentAndAdvance();
  }

  public static ParquelMysteryToken read(ParquelCharacterStream stream) {
    return new ParquelMysteryToken(stream);
  }

  @Override
  public String toString() {
    return ((Character)character).toString();
  }

  public char character() { return this.character; }
}