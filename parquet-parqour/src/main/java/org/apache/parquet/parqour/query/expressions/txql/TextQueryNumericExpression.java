package org.apache.parquet.parqour.query.expressions.txql;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryNumericToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 6/30/15.
 */
public class TextQueryNumericExpression extends TextQueryVariableExpression {
  private TextQueryNumericToken value;

  public TextQueryNumericExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.NUMERIC);

    this.value = readValue(lexer);
  }

  private TextQueryNumericToken readValue(TextQueryLexer lexer) {
    return lexer.readCurrentAndAdvance(TextQueryExpressionType.NUMERIC);
  }

  public static TextQueryNumericExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryNumericExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return value.toString();
  }

  public Integer asInteger() {
    return this.value.value().intValue();
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }
}
