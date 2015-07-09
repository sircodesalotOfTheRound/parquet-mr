package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.infix.ParquelInfixExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 6/30/15.
 */
public class ParquelWhereExpression extends ParquelExpression {
  private final ParquelInfixExpression expression;

  private ParquelWhereExpression(ParquelExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.WHERE);

    lexer.readCurrentAndAdvance(ParquelExpressionType.IDENTIFIER, ParquelKeywordExpression.WHERE);
    this.expression = ParquelInfixExpression.read(this, lexer);
  }

  public static boolean canParse(ParquelSelectStatement parent, ParquelLexer lexer) {
    return lexer.currentIs(ParquelExpressionType.IDENTIFIER, ParquelKeywordExpression.WHERE);
  }

  public static ParquelWhereExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return new ParquelWhereExpression(parent, lexer);
  }

  public ParquelInfixExpression infixExpression() {
    return expression;
  }
}
