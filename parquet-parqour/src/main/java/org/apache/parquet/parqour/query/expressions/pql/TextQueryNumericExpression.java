package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelNumericToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 6/30/15.
 */
public class TextQueryNumericExpression extends TextQueryVariableExpression {
  private ParquelNumericToken value;

  public TextQueryNumericExpression(TextQueryExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.NUMERIC);

    this.value = readValue(lexer);
  }

  private ParquelNumericToken readValue(ParquelLexer lexer) {
    return lexer.readCurrentAndAdvance(ParquelExpressionType.NUMERIC);
  }

  public static TextQueryNumericExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return new TextQueryNumericExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return value.toString();
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
