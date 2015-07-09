package org.apache.parquet.parqour.query.tokens;


import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelCharacterStream;

import java.math.BigInteger;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelNumericToken extends ParquelToken {
  private BigInteger value;

  private ParquelNumericToken(ParquelCharacterStream stream) {
    super(stream.position(), ParquelExpressionType.NUMERIC);
    this.value = parseValueFromString(stream);
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

  public static ParquelNumericToken read(ParquelCharacterStream stream) {
    return new ParquelNumericToken(stream);
  }
}
