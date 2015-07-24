package org.apache.parquet.parqour.query.expressions.variable;

import org.apache.parquet.parqour.query.collections.TextQueryCollection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;

/**
 * Created by sircodesalot on 7/24/15.
 */
public class TextQueryUdfExpression extends TextQueryVariableExpression {
  private final TextQueryFullyQualifiedNameExpression identifier;
  private final TextQueryCollection<TextQueryVariableExpression> parameters;

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

  public TextQueryFullyQualifiedNameExpression functionName() {
    return this.identifier;
  }

  public int parameterCount() {
    return this.parameters.count();
  }

  public TextQueryCollection<TextQueryVariableExpression> parameters() {
    return this.parameters;
  }
}
