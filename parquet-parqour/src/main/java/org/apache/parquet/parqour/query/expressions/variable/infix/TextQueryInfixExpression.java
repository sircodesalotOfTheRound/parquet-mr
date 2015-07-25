package org.apache.parquet.parqour.query.expressions.variable.infix;

import org.apache.parquet.parqour.exceptions.TextQueryException;
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

  private TextQueryInfixExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.INFIX);

    this.lhs = readLhs(lexer);
    this.operationToken = readOperationToken(lexer);
    this.rhs = readRhs(lexer);
  }

  private TextQueryInfixExpression(TextQueryExpression parent, TextQueryInfixExpression lhs, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.INFIX);

    this.lhs = lhs;
    this.operationToken = readOperationToken(lexer);
    this.rhs = readRhsAllowingForInfixExpressions(lexer);
  }

  private TextQueryExpression readLhs(TextQueryLexer lexer) {
    return TextQueryVariableExpression.readIgnoringInfixExpressions(this, lexer);
  }

  private TextQueryToken readOperationToken(TextQueryLexer lexer) {
    if (TextQueryInfixTokens.isInfixToken(lexer)) {
      return lexer.readCurrentAndAdvance();
    } else {
      throw new TextQueryException("Invalid token for infix expression");
    }
  }

  private TextQueryExpression readRhs(TextQueryLexer lexer) {
    return TextQueryVariableExpression.readIgnoringInfixExpressions(this, lexer);
  }

  private TextQueryExpression readRhsAllowingForInfixExpressions(TextQueryLexer lexer) {
    return TextQueryVariableExpression.read(this, lexer);
  }

  public TextQueryExpression lhs() { return this.lhs; }
  public TextQueryExpression rhs() { return this.rhs; }
  public TextQueryToken operator() { return this.operationToken; }

  public static TextQueryInfixExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    TextQueryInfixExpression infixExpression = new TextQueryInfixExpression(parent, lexer);

    if (TextQueryInfixTokens.isInfixToken(lexer)) {
      return new TextQueryInfixExpression(parent, infixExpression, lexer);
    } else {
      return infixExpression;
    }
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
