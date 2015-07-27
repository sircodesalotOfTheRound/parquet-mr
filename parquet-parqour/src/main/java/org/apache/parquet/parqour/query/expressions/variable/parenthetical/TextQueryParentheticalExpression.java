package org.apache.parquet.parqour.query.expressions.variable.parenthetical;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 7/25/15.
 */
public class TextQueryParentheticalExpression extends TextQueryVariableExpression {
  private final TextQueryVariableExpression innerExpression;

  public TextQueryParentheticalExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.PARENTHETICAL);

    this.innerExpression = readInnerExpression(lexer);
  }

  private TextQueryVariableExpression readInnerExpression(TextQueryLexer lexer) {
    lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.OPEN_PARENS);
    TextQueryVariableExpression innerExpression = TextQueryVariableExpression.read(this, lexer);
    lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.CLOSE_PARENS);

    return innerExpression;
  }

  public TextQueryVariableExpression innerExpression() {
    return this.innerExpression;
  }

  public static TextQueryParentheticalExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryParentheticalExpression(parent, lexer);
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    // Elide parenthasis.
    this.innerExpression.setParent(parent);
    return this.innerExpression.simplify(parent);
  }

  @Override
  public TextQueryVariableExpression negate() {
    return null;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }
}
