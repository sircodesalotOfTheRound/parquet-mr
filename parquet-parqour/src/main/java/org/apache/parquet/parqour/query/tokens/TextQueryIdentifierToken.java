package org.apache.parquet.parqour.query.tokens;


import org.apache.parquet.parqour.exceptions.ParquelException;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelCharacterStream;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryIdentifierToken extends TextQueryToken {
  private final String identifier;

  private TextQueryIdentifierToken(ParquelCharacterStream stream) {
    super(stream.position(), TextQueryExpressionType.IDENTIFIER);
    this.identifier = parseIdentifier(stream);
  }

  private String parseIdentifier(ParquelCharacterStream stream) {
    if (!stream.currentIsAlpha()) {
      throw new ParquelException("First character of identifier must be alpha.");
    }

    StringBuilder builder = new StringBuilder();
    while (!stream.isEof()
      && (stream.currentIsAlphaNumeric() || stream.currentIs('_')))
    {
      builder.append(stream.readCurrentAndAdvance());
    }

    return builder.toString();
  }

  public String identifier() { return this.identifier; }

  @Override
  public String toString() {
    return identifier;
  }

  public static TextQueryIdentifierToken read(ParquelCharacterStream stream) {
    return new TextQueryIdentifierToken(stream);
  }
}
