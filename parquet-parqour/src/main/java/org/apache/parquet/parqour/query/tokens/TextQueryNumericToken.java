package org.apache.parquet.parqour.query.tokens;


import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelCharacterStream;

import java.math.BigInteger;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryNumericToken extends TextQueryToken {
  private BigInteger value;

  private TextQueryNumericToken(ParquelCharacterStream stream) {
    super(stream.position(), TextQueryExpressionType.NUMERIC);
    this.value = parseValueFromString(stream);
  }

  public TextQueryNumericToken(BigInteger value) {
    super(null, TextQueryExpressionType.NUMERIC);

    this.value = value;
  }

  private BigInteger parseValueFromString(ParquelCharacterStream stream) {
    StringBuilder builder = new StringBuilder();
    while (!stream.isEof() && (stream.currentIsNumeric() || stream.currentIs('.'))) {
      builder.append(stream.readCurrentAndAdvance());
    }

    return new BigInteger(builder.toString());
  }

  public BigInteger value() { return this.value; }

  @Override
  public String toString() {
    return value.toString();
  }

  public static TextQueryNumericToken read(ParquelCharacterStream stream) {
    return new TextQueryNumericToken(stream);
  }
}
