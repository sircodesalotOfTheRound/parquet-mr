package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 6/30/15.
 */
public class TextQueryWhereExpression extends TextQueryExpression {
  private final TextQueryInfixExpression expression;

  private TextQueryWhereExpression(TextQueryExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.WHERE);

    lexer.readCurrentAndAdvance(ParquelExpressionType.IDENTIFIER, TextQueryKeywordExpression.WHERE);
    this.expression = TextQueryInfixExpression.read(this, lexer);
  }

  public static boolean canParse(TextQuerySelectStatement parent, ParquelLexer lexer) {
    return lexer.currentIs(ParquelExpressionType.IDENTIFIER, TextQueryKeywordExpression.WHERE);
  }

  public static TextQueryWhereExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return new TextQueryWhereExpression(parent, lexer);
  }

  public TextQueryInfixExpression infixExpression() {
    return expression;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }

}
