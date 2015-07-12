package org.apache.parquet.parqour.query.expressions.infix;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 6/30/15.
 */
public class TextQueryInfixExpression extends TextQueryVariableExpression {
  private TextQueryExpression lhs;
  private TextQueryExpression rhs;
  private final TextQueryToken operationToken;

  public TextQueryInfixExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.INFIX);

    this.lhs = readLhs(lexer);
    this.operationToken = readOperationToken(lexer);
    this.rhs = readRhs(lexer);
  }

  private TextQueryExpression readLhs(TextQueryLexer lexer) {
    return TextQueryVariableExpression.read(this, lexer);
  }

  private TextQueryToken readOperationToken(TextQueryLexer lexer) {
    return lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION);
  }

  private TextQueryExpression readRhs(TextQueryLexer lexer) {
    return TextQueryVariableExpression.read(this, lexer);
  }

  public TextQueryExpression lhs() { return this.lhs; }
  public TextQueryExpression rhs() { return this.rhs; }
  public TextQueryToken operator() { return this.operationToken; }

  public static TextQueryInfixExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryInfixExpression(parent, lexer);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    if (lhs != null) {
      lhs.accept(visitor);
    }

    if (rhs != null) {
      rhs.accept(visitor);
    }

    return null;
  }

}
