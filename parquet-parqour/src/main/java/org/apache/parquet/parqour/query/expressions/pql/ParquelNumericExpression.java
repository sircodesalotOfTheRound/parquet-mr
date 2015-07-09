package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.ParquelVariableExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelNumericToken;

/**
 * Created by sircodesalot on 6/30/15.
 */
public class ParquelNumericExpression extends ParquelVariableExpression {
  private ParquelNumericToken value;

  public ParquelNumericExpression(ParquelExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.NUMERIC);

    this.value = readValue(lexer);
  }

  private ParquelNumericToken readValue(ParquelLexer lexer) {
    return lexer.readCurrentAndAdvance(ParquelExpressionType.NUMERIC);
  }

  public static ParquelNumericExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return new ParquelNumericExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return value.toString();
  }
}
