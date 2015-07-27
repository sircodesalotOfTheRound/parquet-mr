package org.apache.parquet.parqour.query.expressions.variable;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.query.collections.TextQueryAppendableCollection;
import org.apache.parquet.parqour.query.collections.TextQueryCollection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 7/24/15.
 */
public class TextQueryUdfExpression extends TextQueryVariableExpression {
  private TextQueryFullyQualifiedNameExpression identifier;
  private TextQueryCollection<TextQueryVariableExpression> parameters;
  private boolean isNegated = false;

  private TextQueryUdfExpression(TextQueryUdfExpression udfExpression, Iterable<TextQueryVariableExpression> simplifiedParameters) {
    super(udfExpression.parent(), TextQueryExpressionType.UDF);

    this.identifier = udfExpression.identifier;
    this.isNegated = udfExpression.isNegated;
    this.parameters = new TextQueryAppendableCollection<>(simplifiedParameters);
  }

  public TextQueryUdfExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.UDF);

    this.identifier = readIdentifier(lexer);
    this.parameters = readParameters(lexer);
  }

  private TextQueryFullyQualifiedNameExpression readIdentifier(TextQueryLexer lexer) {
    return TextQueryFullyQualifiedNameExpression.read(this, lexer);
  }

  private TextQueryCollection<TextQueryVariableExpression> readParameters(TextQueryLexer lexer) {
    lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.OPEN_PARENS);
    TextQueryCollection<TextQueryVariableExpression> parameters = TextQueryVariableExpression.readParameterList(this, lexer);
    lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.CLOSE_PARENS);

    return parameters;
  }

  public static TextQueryUdfExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryUdfExpression(parent, lexer);
  }

  @Override
  public TextQueryVariableExpression simplify(final TextQueryExpression parent) {
    Iterable<TextQueryVariableExpression> simplifiedParameters = this.parameters
      .map(new Projection<TextQueryVariableExpression, TextQueryVariableExpression>() {
        @Override
        public TextQueryVariableExpression apply(TextQueryVariableExpression parameter) {
          return parameter.simplify(parent);
        }
      });

    return new TextQueryUdfExpression(this, simplifiedParameters);
  }

  @Override
  public TextQueryVariableExpression negate() {
    this.isNegated = !isNegated;
    return this;
  }

  public TextQueryFullyQualifiedNameExpression functionName() {
    return this.identifier;
  }

  public int parameterCount() {
    return this.parameters.count();
  }

  public TextQueryCollection<TextQueryVariableExpression> parameters() {
    return this.parameters;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }

  public boolean isNegated() { return this.isNegated; }
}
