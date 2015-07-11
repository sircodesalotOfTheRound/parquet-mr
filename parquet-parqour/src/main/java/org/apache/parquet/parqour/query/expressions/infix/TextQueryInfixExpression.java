package org.apache.parquet.parqour.query.expressions.infix;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 6/30/15.
 */
public class TextQueryInfixExpression extends TextQueryVariableExpression {
  private TextQueryExpression lhs;
  private TextQueryExpression rhs;
  private final ParquelToken operationToken;

  public TextQueryInfixExpression(TextQueryExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.INFIX);

    this.lhs = readLhs(lexer);
    this.operationToken = readOperationToken(lexer);
    this.rhs = readRhs(lexer);
  }

  private TextQueryExpression readLhs(ParquelLexer lexer) {
    return TextQueryVariableExpression.read(this, lexer);
  }

  private ParquelToken readOperationToken(ParquelLexer lexer) {
    return lexer.readCurrentAndAdvance(ParquelExpressionType.PUNCTUATION);
  }

  private TextQueryExpression readRhs(ParquelLexer lexer) {
    return TextQueryVariableExpression.read(this, lexer);
  }

  public TextQueryExpression lhs() { return this.lhs; }
  public TextQueryExpression rhs() { return this.rhs; }
  public ParquelToken operator() { return this.operationToken; }

  public static TextQueryInfixExpression read(TextQueryExpression parent, ParquelLexer lexer) {
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
