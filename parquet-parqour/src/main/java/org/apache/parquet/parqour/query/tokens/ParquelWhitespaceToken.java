package org.apache.parquet.parqour.query.tokens;


import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelCharacterStream;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelWhitespaceToken extends ParquelToken {
  private char whitespaceCharacter;
  private final int length;

  private ParquelWhitespaceToken(ParquelCharacterStream stream) {
    super(stream.position(), ParquelExpressionType.WHITESPACE);
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

  public static ParquelWhitespaceToken read(ParquelCharacterStream stream) {
    return new ParquelWhitespaceToken(stream);
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
