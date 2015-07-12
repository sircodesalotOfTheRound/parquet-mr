package org.apache.parquet.parqour.query.tokens;


import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelCharacterStream;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryWhitespaceToken extends TextQueryToken {
  private char whitespaceCharacter;
  private final int length;

  private TextQueryWhitespaceToken(ParquelCharacterStream stream) {
    super(stream.position(), TextQueryExpressionType.WHITESPACE);
    this.whitespaceCharacter = stream.current();
    this.length = parseWhitespaceLength(stream);
  }

  private int parseWhitespaceLength(ParquelCharacterStream stream) {
    char whitespaceCharacter = stream.current();
    int length = 0;

    while (!stream.isEof() && stream.currentIs(whitespaceCharacter)) {
      stream.readCurrentAndAdvance();
      length += 1;
    }

    return length;
  }

  public static TextQueryWhitespaceToken read(ParquelCharacterStream stream) {
    return new TextQueryWhitespaceToken(stream);
  }

  public char whitespaceCharacter() { return whitespaceCharacter; }
  public int length() { return length; }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (int index = 0; index < length; index++) {
      builder.append(whitespaceCharacter);
    }
    return builder.toString();
  }
}
